# Keyv-secondary

![NPM Version](https://img.shields.io/npm/v/keyv-secondary)

[Keyv](https://keyv.org) with secondary indexes

## Example

```typescript
import assert from 'node:assert'
import { KeyvSecondary } from 'keyv-secondary'

type Id = number & {__brand: 'id'}
type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byYoungAge' | 'byOldAge'

const kv = new KeyvSecondary<Id, Person, Indexes>(
  {}, // Keyv options
  {
    indexes: [
      {
        field: 'age',
        filter: ({ age }) => age >= 40,
        name: 'byOldAge',
      },
      {
        field: 'age',
        filter: ({ age }) => age < 40,
        name: 'byYoungAge',
      },
    ],
  }
)

await kv.set(1 as Id, { age: 30, firstName: 'Galina', id: 1 as Id, lastName: 'Ivanova' })
await kv.set(2 as Id, { age: 59, firstName: 'Zinaida', id: 2 as Id, lastName: 'Petrovna' })
await kv.set(3 as Id, { age: 17, firstName: 'Stepan', id: 3 as Id, lastName: 'Lukov' })
await kv.set(4 as Id, { age: 59, firstName: 'Ibragim', id: 4 as Id, lastName: 'Lukov' })

assert.deepStrictEqual(await kv.getByIndex('byOldAge', 59), [
  {
    age: 59,
    firstName: 'Zinaida',
    id: 2,
    lastName: 'Petrovna',
  },
  {
    age: 59,
    firstName: 'Ibragim',
    id: 4,
    lastName: 'Lukov',
  },
])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 30), [{ age: 30, firstName: 'Galina', id: 1, lastName: 'Ivanova' }])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 17), [{ age: 17, firstName: 'Stepan', id: 3, lastName: 'Lukov' }])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 59), [])

assert.deepStrictEqual(await kv.getByIndex('byOldAge', 17), [])
```

## Concurrency Handling

Internally, `KeyvSecondary` uses a `p-queue` with a concurrency limit of 1 to ensure that operations such as `set` and `delete` are processed in a serialized manner. This avoids race conditions when multiple asynchronous operations are performed on the same instance.

If you want to provide your own concurrency control mechanism, you can use the `options.locker` parameter:

```typescript
const customLocker = async <T>(cb: () => T): T => {
  // Custom concurrency logic
  return cb()
}

const kv = new KeyvSecondary<Id, Person, Indexes>(
  {}, // Keyv options
  {
    locker: customLocker, // Pass your custom locker function
    // ... indexes
  }
)
```

By default, if no custom `locker` is provided, the internal `p-queue` will be used.

## Index generation by field example for Postgres

```sql
select
  'keyv:$secondary-index:byCountry:'
    || (value->'value'->>'country')::text as key,
  jsonb_build_object(
    'value',
    array_agg(trim('keyv:' from key)::int)
  ) as value
from keyv_user 
where not key like '%$secondary-index%'
group by value->'value'->>'country'
```
