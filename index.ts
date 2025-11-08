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
      for (const { field, filter, name } of this.indexes) {
        if (oldVal != null) {
          const keys = await this.get<string[]>(`$secondary-index:${name}:${oldVal[field]}`)
          if (keys) {
            await super.set(`$secondary-index:${name}:${oldVal[field]}`, keys.filter(k => k !== key) as V)
          }
        }
        if (filter(value as unknown as V)) {
          const keys = await this.get<string[]>(`$secondary-index:${name}:${(value as unknown as V)[field]}`)
          await super.set(
            `$secondary-index:${name}:${(value as unknown as V)[field]}`,
            Array.from(new Set(keys ?? []).add(key)) as V
          )
        }
      }
      return await super.set(key, value, ttl)
    })
  }

  override async delete(key: string) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      if (oldVal != null) {
        for (const { name, field } of this.indexes) {
          const keys = await this.get<string[]>(`$secondary-index:${name}:${oldVal[field]}`)
          if (!keys) {
            continue
          }
          await super.set(`$secondary-index:${name}:${oldVal[field]}`, keys.filter(k => k !== key) as V)
        }
      }
      return await super.delete(key)
    })
  }

  private locker<T>(cb: () => T) {
    return this.pQueue.add<T>(cb)
  }
}
