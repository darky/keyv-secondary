# indexable-map

![NPM Version](https://img.shields.io/npm/v/indexable-map)

Built-in JavaScript Map with secondary indexes

## Example

```typescript
import assert from 'node:assert'
import { IndexableMap } from 'indexable-map'

type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byYoungAge' | 'byOldAge'

const im = new IndexableMap<number, Person, Indexes>(
  [
    [1, { age: 30, firstName: 'Galina', lastName: 'Ivanova' }],
    [2, { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' }],
    [3, { age: 17, firstName: 'Stepan', lastName: 'Lukov' }],
    [4, { age: 59, firstName: 'Ibragim', lastName: 'Lukov' }],
  ],
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

assert.deepStrictEqual(im.getByIndex('byOldAge', 59), [
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

assert.deepStrictEqual(im.getByIndex('byYoungAge', 30), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])

assert.deepStrictEqual(im.getByIndex('byYoungAge', 17), [{ age: 17, firstName: 'Stepan', lastName: 'Lukov' }])

assert.deepStrictEqual(im.getByIndex('byYoungAge', 59), [])

assert.deepStrictEqual(im.getByIndex('byOldAge', 17), [])
```
