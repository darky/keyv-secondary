import { Keyv, type KeyvOptions } from 'keyv'
import PQueue from 'p-queue'

const objKeys = Object.keys.bind(Object) as <K extends string>(obj: {
  [key in K]: unknown
}) => K[]

export class KeyvSecondary<V, I extends string> extends Keyv<V> {
  private _indexFields = {} as {
    [indexName in I]: keyof V
  }
  private _indexFilters = {} as {
    [indexName in I]: (val: V) => boolean
  }

  private pQueue = new PQueue({ concurrency: 1 })
  private passedLocker?: <T>(cb: () => T) => T

  constructor(
    keyvOptions?: KeyvOptions,
    options?: { indexes?: { field: keyof V; filter: (val: V) => boolean; name: I }[]; locker?: <T>(cb: () => T) => T }
  ) {
    super(keyvOptions)
    for (const { field, filter, name } of options?.indexes ?? []) {
      this._indexFields[name] = field
      this._indexFilters[name] = filter
    }
    if (options?.locker) {
      this.passedLocker = options.locker
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
      for (const indexName of objKeys(this._indexFields)) {
        if (oldVal != null) {
          const keys = await this.get<string[]>(`$secondary-index:${indexName}:${oldVal[this._indexFields[indexName]]}`)
          if (keys) {
            await super.set(
              `$secondary-index:${indexName}:${oldVal[this._indexFields[indexName]]}`,
              keys.filter(k => k !== key) as V
            )
          }
        }
        if (this._indexFilters[indexName](value as unknown as V)) {
          const keys = await this.get<string[]>(
            `$secondary-index:${indexName}:${(value as unknown as V)[this._indexFields[indexName]]}`
          )
          await super.set(
            `$secondary-index:${indexName}:${(value as unknown as V)[this._indexFields[indexName]]}`,
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
        for (const indexName of objKeys(this._indexFields)) {
          const keys = await this.get<string[]>(`$secondary-index:${indexName}:${oldVal[this._indexFields[indexName]]}`)
          if (!keys) {
            continue
          }
          await super.set(
            `$secondary-index:${indexName}:${oldVal[this._indexFields[indexName]]}`,
            keys.filter(k => k !== key) as V
          )
        }
      }
      return await super.delete(key)
    })
  }

  private locker<T>(cb: () => T) {
    return this.passedLocker?.(cb) ?? this.pQueue.add<T>(cb)
  }
}
