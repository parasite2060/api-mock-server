import { Kafka, logLevel, Partitioners, type Admin, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import { decodeMessage, react } from './reactor';
import type { Recorder } from './recorder';
import { SubscribeTimeoutError, type BridgeLike } from './routes';
import { MOCK_ORIGIN_HEADER, MOCK_ORIGIN_VALUE, type OutgoingMessage } from './types';

export interface KafkaBridgeOptions {
  brokers: string[];
  clientId: string;
  recorder: Recorder;
  subscribeTimeoutMs?: number;
  reconnectMs?: number;
}

const errMessage = (e: unknown): string => (e instanceof Error ? e.message : String(e));

export class KafkaBridge implements BridgeLike {
  private readonly admin: Admin;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly recorder: Recorder;
  private readonly subscribeTimeoutMs: number;
  private readonly reconnectMs: number;

  private isConnected = false;
  private stopped = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly subscribed = new Set<string>();
  /** "topic:partition" → lowest offset still to be processed. Offsets below it are backlog or redelivery. */
  private readonly floor = new Map<string, bigint>();
  private queue: Promise<void> = Promise.resolve();
  /** Settles once every earlier re-subscribe, including ones whose caller already timed out, has finished. */
  private settling: Promise<void> = Promise.resolve();
  private abortInflight: ((e: Error) => void) | null = null;

  constructor(opts: KafkaBridgeOptions) {
    const kafka = new Kafka({ clientId: opts.clientId, brokers: opts.brokers, logLevel: logLevel.ERROR });
    // The admin connects first. It gets its own client with a short connect retry so an unreachable broker fails in
    // well under a second and start() retries every ~reconnectMs (kafkajs's default connect retry takes ~10 s).
    // Admin operations keep kafkajs's default 5 retries.
    const adminKafka = new Kafka({ clientId: opts.clientId, brokers: opts.brokers, logLevel: logLevel.ERROR, retry: { retries: 1, initialRetryTime: 300 } });
    this.admin = adminKafka.admin({ retry: { retries: 5 } });
    this.producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    // consumer.stop() waits for the in-flight fetch; a short maxWaitTimeInMs (default 5000) keeps re-subscribes fast.
    this.consumer = kafka.consumer({ groupId: `api-mock-server-${crypto.randomUUID()}`, maxWaitTimeInMs: 500 });
    this.recorder = opts.recorder;
    this.subscribeTimeoutMs = opts.subscribeTimeoutMs ?? 30000;
    this.reconnectMs = opts.reconnectMs ?? 5000;
  }

  get connected(): boolean {
    return this.isConnected;
  }

  get topics(): string[] {
    return [...this.subscribed];
  }

  /** Connects admin, producer and consumer. Never throws: on failure it retries every `reconnectMs` in the background. */
  async start(): Promise<void> {
    if (this.stopped || this.isConnected) return;
    try {
      await this.admin.connect();
      await this.producer.connect();
      await this.consumer.connect();
      if (this.stopped) {
        await this.disconnectAll();
        return;
      }
      this.isConnected = true;
    } catch (e) {
      console.error(`[kafka] connect failed: ${errMessage(e)}; retrying in ${this.reconnectMs}ms`);
      await this.disconnectAll();
      if (!this.stopped) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          void this.start();
        }, this.reconnectMs);
      }
    }
  }

  ensureSubscribed(topics: string[]): Promise<void> {
    const run = this.queue.then(() => this.subscribe(topics));
    this.queue = run.catch(() => {});
    return run;
  }

  private async subscribe(topics: string[]): Promise<void> {
    const missing = [...new Set(topics)].filter((t) => !this.subscribed.has(t));
    if (missing.length === 0) return;
    if (!this.isConnected) throw new Error('kafka bridge is not connected');

    // One deadline bounds the whole operation. The background work may outlive it (consumer.stop() waits for an
    // in-flight handler, e.g. one sleeping on delay_ms), so the caller gets its rejection on time while the work
    // finishes on its own; the next subscribe waits for it before touching the consumer.
    let aborted = false;
    let abort!: (e: Error) => void;
    const abortion = new Promise<never>((_, reject) => {
      abort = (e) => { aborted = true; reject(e); };
    });
    abortion.catch(() => {});
    const timer = setTimeout(() => abort(new SubscribeTimeoutError(missing)), this.subscribeTimeoutMs);
    this.abortInflight = abort;

    const previous = this.settling;
    const work = this.resubscribe(previous, missing, abortion, () => aborted);
    this.settling = Promise.allSettled([previous, work]).then(() => {});
    try {
      await Promise.race([work, abortion]);
    } finally {
      clearTimeout(timer);
      if (this.abortInflight === abort) this.abortInflight = null;
    }
    for (const t of missing) this.subscribed.add(t);
  }

  private async resubscribe(previous: Promise<void>, missing: string[], abortion: Promise<never>, isAborted: () => boolean): Promise<void> {
    await Promise.race([previous, abortion]);

    // Only create topics that don't exist yet: kafkajs logs an ERROR for TOPIC_ALREADY_EXISTS even though it's harmless.
    const existing = new Set(await this.admin.listTopics());
    const toCreate = missing.filter((t) => !existing.has(t));
    if (toCreate.length > 0) {
      await this.admin.createTopics({ topics: toCreate.map((topic) => ({ topic })), waitForLeaders: true });
    }
    // Capture the start floor before subscribing: anything already on the topic is backlog.
    for (const topic of missing) {
      for (const { partition, high } of await this.admin.fetchTopicOffsets(topic)) {
        this.floor.set(`${topic}:${partition}`, BigInt(high));
      }
    }
    // Past this point the consumer is stopped and must be restarted, so give up only before touching it.
    if (isAborted()) return;

    const all = [...this.subscribed, ...missing];
    let removeListener: () => void = () => {};
    const joined = new Promise<void>((resolve) => {
      removeListener = this.consumer.on(this.consumer.events.GROUP_JOIN, (e) => {
        const assigned = e.payload.memberAssignment;
        if (all.every((t) => assigned[t] !== undefined)) resolve();
      });
    });
    try {
      await this.consumer.stop();
      await this.consumer.subscribe({ topics: all, fromBeginning: true });
      await this.consumer.run({ eachMessage: (p) => this.handle(p) });
      await Promise.race([joined, abortion]);
    } finally {
      removeListener();
    }
  }

  private async handle({ topic, partition, message }: EachMessagePayload): Promise<void> {
    try {
      const key = `${topic}:${partition}`;
      const offset = BigInt(message.offset);
      if (offset < (this.floor.get(key) ?? 0n)) return;
      // Advance before reacting so a redelivery (e.g. after a rebalance) is never handled twice.
      this.floor.set(key, offset + 1n);
      this.recorder.record(await react(decodeMessage(topic, partition, message), (m) => this.publish(m)));
    } catch (e) {
      console.error(`[kafka] failed to handle ${topic}[${partition}]@${message.offset}: ${errMessage(e)}`);
    }
  }

  async publish(msg: OutgoingMessage): Promise<{ topic: string; partition: number; offset: string }> {
    const [md] = await this.producer.send({
      topic: msg.topic,
      messages: [{
        key: msg.key,
        value: JSON.stringify(msg.value),
        headers: { ...msg.headers, [MOCK_ORIGIN_HEADER]: MOCK_ORIGIN_VALUE },
      }],
    });
    if (!md) throw new Error(`no record metadata returned for topic ${msg.topic}`);
    return { topic: md.topicName, partition: md.partition, offset: md.baseOffset ?? md.offset ?? '' };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.isConnected = false;
    this.abortInflight?.(new Error('kafka bridge stopped'));
    await this.disconnectAll();
  }

  private async disconnectAll(): Promise<void> {
    await this.consumer.disconnect().catch(() => {});
    await this.producer.disconnect().catch(() => {});
    await this.admin.disconnect().catch(() => {});
  }
}
