import { Keyv, type KeyvOptions } from 'keyv'
import { AsyncLocalStorage } from 'node:async_hooks'
import PQueue from 'p-queue'

export class KeyvSecondary<K, V, I extends string> extends Keyv<V> {
  private pQueue = new PQueue({ concurrency: 1 })
  private lockerContext = new AsyncLocalStorage()
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
      this.locker = cb => {
        const inLockContext = this.lockerContext.getStore()
        if (inLockContext) {
          return cb()
        }
        return options.locker!(cb)
      }
    }
    if (options?.indexes) {
      this.indexes = options.indexes
    }
  }

  async getByIndex<Key extends keyof V>(name: I, value: V[Key]) {
    const keys = (await this.get(`$secondary-index:${name}:${value}` as K)) as K[] | void
    if (!keys) {
      return []
    }
    return this.getMany(keys)
  }

  // @ts-ignore
  override get(key: K) {
    return super.get(String(key))
  }

  // @ts-ignore
  override getMany(keys: K[]) {
    return super.getMany(keys.map(k => String(k)))
  }

  // @ts-ignore
  override setMany(
    entities: {
      key: K
      value: V
      ttl?: number
    }[]
  ) {
    return super.setMany(
      entities.map(({ key, value, ttl }) => ({
        key: String(key),
        value,
        ttl: ttl as number,
      }))
    )
  }

  // @ts-ignore
  override async set(key: K, value: V, ttl?: number) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      const newVal = value as unknown as V
      for (const { field, filter, name } of this.indexes) {
        if (oldVal != null) {
          await this.deleteFromIndex(key, name, oldVal[field] as V)
        }
        if (filter(newVal)) {
          const keys = (await this.get(`$secondary-index:${name}:${newVal[field]}` as K)) as K[] | void
          await super.set(`$secondary-index:${name}:${newVal[field]}`, [...new Set(keys ?? []).add(key)])
        }
      }
      return await super.set(String(key), newVal, ttl)
    })
  }

  // @ts-ignore
  override async delete(key: K) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      if (oldVal != null) {
        for (const { name, field } of this.indexes) {
          await this.deleteFromIndex(key, name, oldVal[field] as V)
        }
      }
      return await super.delete(String(key))
    })
  }

  // @ts-ignore
  override async deleteMany(keys: K[]) {
    return this.locker(async () => {
      const oldsVals = await this.getMany(keys)
      for (const { name, field } of this.indexes) {
        let i = 0
        for (const key of keys) {
          if (oldsVals[i] != null) {
            await this.deleteFromIndex(key, name, oldsVals[i]![field] as V)
          }
          i++
        }
      }
      return super.deleteMany(keys.map(k => String(k)))
    })
  }

  private async deleteFromIndex(key: K, name: string, value: V) {
    const keys = (await this.get(`$secondary-index:${name}:${value}` as K)) as K[] | void
    if (keys) {
      await super.set(
        `$secondary-index:${name}:${value}`,
        keys.filter(k => k !== key)
      )
    }
  }

  private locker<T>(cb: () => T) {
    const inLockContext = this.lockerContext.getStore()
    if (inLockContext) {
      return cb()
    }
    return this.lockerContext.run(true, () => this.pQueue.add<T>(cb))
  }
}
