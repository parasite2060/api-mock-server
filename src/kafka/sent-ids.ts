/**
 * Ids of messages this process has published, keyed to the topic they were sent to. A consumed message is the mock's
 * own only if its id is claimed here on the same topic, so an application that copies the mock's headers onto its own
 * messages (tracing middleware, header propagation) is still treated as an application.
 */
export class SentIds {
  /** Insertion-ordered, so the first key is always the oldest. */
  private readonly ids = new Map<string, string>();

  constructor(private readonly capacity = 10000) {}

  get size(): number {
    return this.ids.size;
  }

  remember(id: string, topic: string): void {
    this.ids.set(id, topic);
    while (this.ids.size > this.capacity) {
      const oldest = this.ids.keys().next().value as string;
      this.ids.delete(oldest);
    }
  }

  forget(id: string): void {
    this.ids.delete(id);
  }

  /** True, and the id is forgotten, if `id` was sent to `topic`. Each id is claimed at most once. */
  claim(id: string, topic: string): boolean {
    if (this.ids.get(id) !== topic) return false;
    this.ids.delete(id);
    return true;
  }
}
