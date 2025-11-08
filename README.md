# Keyv-secondary

![NPM Version](https://img.shields.io/npm/v/keyv-secondary)

[Keyv](https://keyv.org) with secondary indexes

## Example

```typescript
import assert from 'node:assert'
import { KeyvSecondary } from 'keyv-secondary'

type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byYoungAge' | 'byOldAge'

const kv = new KeyvSecondary<Person, Indexes>(
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
    ]
  }
)

await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

assert.deepStrictEqual(await kv.getByIndex('byOldAge', 59), [
  {
    age: 59,
    firstName: 'Zinaida',
    lastName: 'Petrovna',
  },
  {
    age: 59,
    firstName: 'Ibragim',
    lastName: 'Lukov',
  },
])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 30), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 17), [{ age: 17, firstName: 'Stepan', lastName: 'Lukov' }])

assert.deepStrictEqual(await kv.getByIndex('byYoungAge', 59), [])

assert.deepStrictEqual(await kv.getByIndex('byOldAge', 17), [])
```
