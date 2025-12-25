import { Keyv, type KeyvOptions } from 'keyv'
import { AsyncLocalStorage } from 'node:async_hooks'
import PQueue from 'p-queue'

export class KeyvSecondary<K, V, I extends string> extends Keyv<V> {
  private pQueue = new PQueue({ concurrency: 1 })
  private lockerContext = new AsyncLocalStorage<true>()
  private indexes: { field?: keyof V; filter: (val: V) => boolean; map?: (val: V) => unknown; name: I }[] = []

  constructor(
    keyvOptions?: KeyvOptions,
    options?: {
      indexes?: { field?: keyof V; filter: (val: V) => boolean; map?: (val: V) => unknown; name: I }[]
      locker?: <T>(cb: () => T) => Promise<T>
    }
  ) {
    for (const { field, map, name } of options?.indexes ?? []) {
      if (field == null && map == null) {
        throw new Error(`On index "${name}" need to setup 'field' or 'map'`)
      }
    }
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

  async getByIndex(name: I, value: unknown) {
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
  override has(key: K) {
    return super.has(String(key))
  }

  // @ts-ignore
  override hasMany(keys: K[]) {
    return super.hasMany(keys.map(k => String(k)))
  }

  // @ts-ignore
  override async set(key: K, value: V, ttl?: number) {
    return this.locker(async () => {
      if (String(key).match(/\$secondary\-index/)) {
        return await super.set(String(key), value, ttl)
      }
      const oldVal = await this.get(key)
      const newVal = value as unknown as V
      for (const { field, filter, map, name } of this.indexes) {
        if (oldVal != null) {
          await this.deleteFromIndex(key, name, field ? oldVal[field] : map!(oldVal))
        }
        if (filter(newVal)) {
          const val = field ? newVal[field] : map!(newVal)
          const keys = (await this.get(`$secondary-index:${name}:${val}` as K)) as K[] | void
          await super.set(`$secondary-index:${name}:${val}`, [...new Set(keys ?? []).add(key)])
        }
      }
      return await super.set(String(key), newVal, ttl)
    })
  }

  // @ts-ignore
  override setMany(
    entities: {
      key: K
      value: V
      ttl?: number
    }[]
  ) {
    const entitiesToSet = entities.filter(({ key }) => !String(key).match(/\$secondary\-index/))
    const entitiesToNativeSet = entities
      .filter(({ key }) => String(key).match(/\$secondary\-index/))
      .map(({ key, value, ttl }) => ({
        key: String(key),
        value,
        ttl: ttl as number,
      }))
    return this.locker(async () => {
      if (entitiesToNativeSet.length) {
        await super.setMany(entitiesToNativeSet)
      }
      if (!entitiesToSet.length) {
        return []
      }
      for (const { key, value } of entitiesToSet) {
        const oldVal = await this.get(key)
        const newVal = value as unknown as V
        for (const { field, filter, map, name } of this.indexes) {
          if (oldVal != null) {
            await this.deleteFromIndex(key, name, field ? oldVal[field] : map!(oldVal))
          }
          if (filter(newVal)) {
            const val = field ? newVal[field] : map!(newVal)
            const keys = (await this.get(`$secondary-index:${name}:${val}` as K)) as K[] | void
            await super.set(`$secondary-index:${name}:${val}`, [...new Set(keys ?? []).add(key)])
          }
        }
      }
      return super.setMany(
        entitiesToSet.map(({ key, value, ttl }) => ({
          key: String(key),
          value,
          ttl: ttl as number,
        }))
      )
    })
  }

  // @ts-ignore
  override async delete(key: K) {
    return this.locker(async () => {
      const oldVal = await this.get(key)
      if (oldVal != null) {
        for (const { field, map, name } of this.indexes) {
          await this.deleteFromIndex(key, name, field ? oldVal[field] : map!(oldVal))
        }
      }
      return await super.delete(String(key))
    })
  }

  // @ts-ignore
  override async deleteMany(keys: K[]) {
    return this.locker(async () => {
      const values = await this.getMany(keys)
      const secondaryIndexWithoutKey = this.indexes.flatMap(({ field, map, name }) => {
        return keys.flatMap((primaryKeyForRemove, i) => {
          return values[i] == null
            ? []
            : {
                primaryKeyForRemove,
                secondaryIndexKey: `$secondary-index:${name}:${field ? values[i][field] : map!(values[i])}`,
              }
        })
      })
      const secondaryIndexValues = await super.getMany<K[]>(
        secondaryIndexWithoutKey.map(({ secondaryIndexKey }) => secondaryIndexKey)
      )
      const newSecondaryIndexes = secondaryIndexWithoutKey.flatMap(({ primaryKeyForRemove, secondaryIndexKey }, i) =>
        secondaryIndexValues[i] == null
          ? []
          : {
              key: secondaryIndexKey,
              value: secondaryIndexValues[i].filter(k => k !== primaryKeyForRemove),
            }
      )
      await super.setMany(newSecondaryIndexes)
      return await super.deleteMany(keys.map(k => String(k)))
    })
  }

  private async deleteFromIndex(key: K, name: string, value: unknown) {
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
