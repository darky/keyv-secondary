import { Keyv, type KeyvOptions, type KeyvStoreAdapter } from 'keyv'

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

  constructor(
    store?: KeyvStoreAdapter | KeyvOptions | Map<any, any>,
    options?: Omit<KeyvOptions, 'store'>,
    indexes: { field: keyof V; filter: (val: V) => boolean; name: I }[] = []
  ) {
    super(store, options)
    for (const { field, filter, name } of indexes) {
      this._indexFields[name] = field
      this._indexFilters[name] = filter
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
    return super.set(key, value, ttl)
  }

  override async delete(key: string) {
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
    return super.delete(key)
  }
}
