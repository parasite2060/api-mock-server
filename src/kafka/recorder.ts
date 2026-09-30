import type { RecordedMessage } from './types';

interface Waiter {
  topic: string | undefined;
  min: number;
  resolve: (msgs: RecordedMessage[]) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class Recorder {
  private messages: RecordedMessage[] = [];
  private waiters: Waiter[] = [];

  constructor(private readonly capacity = 1000) {}

  /** Number of waitFor() calls still pending (for leak checks). */
  get pendingWaiters(): number {
    return this.waiters.length;
  }

  record(m: RecordedMessage): void {
    this.messages.push(m);
    if (this.messages.length > this.capacity) {
      this.messages.splice(0, this.messages.length - this.capacity);
    }
    for (const w of [...this.waiters]) {
      if (this.list(w.topic).length >= w.min) this.settle(w);
    }
  }

  list(topic?: string): RecordedMessage[] {
    return topic === undefined ? [...this.messages] : this.messages.filter((m) => m.topic === topic);
  }

  waitFor(topic: string | undefined, min: number, timeoutMs: number): Promise<RecordedMessage[]> {
    if (this.list(topic).length >= min || timeoutMs <= 0) return Promise.resolve(this.list(topic));
    return new Promise((resolve) => {
      const w: Waiter = {
        topic, min, resolve,
        timer: setTimeout(() => this.settle(w), timeoutMs),
      };
      this.waiters.push(w);
    });
  }

  /** Drops recorded messages. Pending waiters are kept and resolve at their timeout (or when satisfied by new messages). */
  clear(): void {
    this.messages = [];
  }

  private settle(w: Waiter): void {
    clearTimeout(w.timer);
    const i = this.waiters.indexOf(w);
    if (i >= 0) this.waiters.splice(i, 1);
    w.resolve(this.list(w.topic));
  }
}
