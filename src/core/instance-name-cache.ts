export const DEFAULT_INSTANCE_NAME_CACHE_SIZE = 1_000;

/** Small process-local LRU hint cache; status APIs still accept explicit names. */
export class InstanceNameCache {
  private readonly entries = new Map<string, string>();

  constructor(
    private readonly maxEntries = DEFAULT_INSTANCE_NAME_CACHE_SIZE,
  ) {
    if (!Number.isSafeInteger(maxEntries) || maxEntries < 0) {
      throw new Error("instanceNameCacheSize must be a non-negative integer");
    }
  }

  get(id: string): string | undefined {
    const name = this.entries.get(id);
    if (name === undefined) return undefined;

    // Refresh recency without allocating another cache entry.
    this.entries.delete(id);
    this.entries.set(id, name);
    return name;
  }

  set(id: string, name: string): void {
    if (this.maxEntries === 0) return;

    this.entries.delete(id);
    this.entries.set(id, name);
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }
}
