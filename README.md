# Keyv-secondary

![NPM Version](https://img.shields.io/npm/v/keyv-secondary)

[Keyv](https://keyv.org) with secondary indexes

## Features

- **Field-based indexing**: Index by any field in your values
- **Custom mapping**: Use `map` function for computed or transformed indexes
- **Filtered indexes**: Only index values that match your filter criteria
- **Concurrency safety**: Built-in locking mechanism prevents race conditions
- **TypeScript support**: Full type safety with generics

## Basic Example

```typescript
import { KeyvSecondary } from 'keyv-secondary'

type Id = number & { __brand: 'id' }
type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byAge' | 'byLastName'

const kv = new KeyvSecondary<Id, Person, Indexes>(
  {}, // Keyv options
  {
    indexes: [
      {
        field: 'age',
        filter: () => true,
        name: 'byAge',
      },
      {
        field: 'lastName',
        filter: () => true,
        name: 'byLastName',
      },
    ],
  }
)

await kv.set(1 as Id, { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
await kv.set(2 as Id, { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })

// Get all people with last name "Petrovna"
const petrovnas = await kv.getByIndex('byLastName', 'Petrovna')
```

## Advanced Indexing with `map`

You can use the `map` function to create indexes based on computed or transformed values:

```typescript
import { KeyvSecondary } from 'keyv-secondary'

type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byInitial' | 'byAgeGroup'

const kv = new KeyvSecondary<string, Person, Indexes>(
  {},
  {
    indexes: [
      {
        // Index by first letter of firstName
        map: person => person.firstName[0],
        filter: () => true,
        name: 'byInitial',
      },
      {
        // Index by age group (adult/minor)
        map: person => (person.age >= 18 ? 'adult' : 'minor'),
        filter: () => true,
        name: 'byAgeGroup',
      },
    ],
  }
)

await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
await kv.set('2', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })

// Get all adults
const adults = await kv.getByIndex('byAgeGroup', 'adult')

// Get all people whose first name starts with 'G'
const gNames = await kv.getByIndex('byInitial', 'G')
```

## Multiple Indexes on Same Field

```typescript
type Person = { age: number; firstName: string; lastName: string }
type Indexes = 'byYoungAge' | 'byOldAge'

const kv = new KeyvSecondary<string, Person, Indexes>(
  {},
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

await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })

const youngPeople = await kv.getByIndex('byYoungAge', 30)
const oldPeople = await kv.getByIndex('byOldAge', 59)
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

## Index Generation for Postgres

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
