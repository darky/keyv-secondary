import { Keyv, type KeyvOptions } from 'keyv'
import PQueue from 'p-queue'

export class KeyvSecondary<V, I extends string> extends Keyv<V> {
  private pQueue = new PQueue({ concurrency: 1 })
  private indexes: { field: keyof V; filter: (val: V) => boolean; name: I }[] = []

  constructor(
    keyvOptions?: KeyvOptions,
    options?: {
      indexes?: { field: keyof V; filter: (val: V) => boolean; name: I }[]
      locker?: <T>(cb: () => T) => Promise<T>
    }
  ) {
    super(keyvOptions)
    if (options?.locker) {
      this.locker = options.locker
    }
    if (options?.indexes) {
      this.indexes = options.indexes
    }
  }

  async getByIndex<K extends keyof V>(name: I, value: V[K]) {
    const keys = await this.get<string[]>(`$secondary-index:${name}:${value}`)
    if (!keys) {
      return []
    }
    return this.getMany(keys)
  }

  override async set<Value>(key: string, value: Value, ttl?: number) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      const newVal = value as unknown as V
      for (const { field, filter, name } of this.indexes) {
        if (oldVal != null) {
          await this.deleteFromIndex(key, name, oldVal[field] as V)
        }
        if (filter(newVal)) {
          const keys = await this.get<string[]>(`$secondary-index:${name}:${newVal[field]}`)
          await super.set(`$secondary-index:${name}:${newVal[field]}`, [...new Set(keys ?? []).add(key)])
        }
      }
      return await super.set(key, newVal, ttl)
    })
  }

  override async delete(key: string) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      if (oldVal != null) {
        for (const { name, field } of this.indexes) {
          await this.deleteFromIndex(key, name, oldVal[field] as V)
        }
      }
      return await super.delete(key)
    })
  }

  private async deleteFromIndex(key: string, name: string, value: V) {
    const keys = await this.get<string[]>(`$secondary-index:${name}:${value}`)
    if (keys) {
      await super.set(
        `$secondary-index:${name}:${value}`,
        keys.filter(k => k !== key)
      )
    }
  }

  private locker<T>(cb: () => T) {
    return this.pQueue.add<T>(cb)
  }
}
