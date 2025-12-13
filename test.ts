import test from 'node:test'
import { KeyvSecondary } from './index.ts'
import assert from 'node:assert'

test('should construct KeyvSecondary', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
  ])
})

test('should construct KeyvSecondary with multiple values for same secondary index', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])
})

test('should construct KeyvSecondary with filtered secondary index', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter({ age }) {
            return age > 30
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter({ lastName }) {
            return lastName.startsWith('L')
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])
})

test('should properly set', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])

  await kv.set('4', { age: 40, firstName: 'Elena', lastName: 'Korchagina' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":40,"firstName":"Elena","lastName":"Korchagina"}}'],
    ['keyv:$secondary-index:byAge:40', '{"value":["4"]}'],
    ['keyv:$secondary-index:byLastName:Korchagina', '{"value":["4"]}'],
  ])
})

test('should properly del', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])

  await kv.delete('4')

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3"]}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
  ])
})

test('should properly get by index', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  assert.deepStrictEqual(await kv.getByIndex('byAge', 59), [
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
  assert.deepStrictEqual(await kv.getByIndex('byLastName', 'Petrovna'), [
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byLastName', 'Zaiceva'), [])
})

test('multiple indexes for same field', async () => {
  type Person = { age: number; firstName: string; lastName: string }
  type Indexes = 'byYoungAge' | 'byOldAge'

  const kv = new KeyvSecondary<string, Person, Indexes>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter({ age }) {
            return age >= 40
          },
          name: 'byOldAge',
        },
        {
          field: 'age',
          filter({ age }) {
            return age < 40
          },
          name: 'byYoungAge',
        },
      ],
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
})

test('should construct KeyvSecondary correctly with setMany', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '2', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
    { key: '3', value: { age: 17, firstName: 'Stepan', lastName: 'Lukov' } },
    { key: '4', value: { age: 59, firstName: 'Ibragim', lastName: 'Lukov' } },
  ])

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])
})

test('should construct KeyvSecondary correctly with setMany without locker', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
      locker: async cb => cb(), // identity fn, no mutex logic
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '2', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
    { key: '3', value: { age: 17, firstName: 'Stepan', lastName: 'Lukov' } },
    { key: '4', value: { age: 59, firstName: 'Ibragim', lastName: 'Lukov' } },
  ])

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])
})

test('should properly setMany', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
      locker: async cb => cb(), // identity fn, no mutex logic
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '2', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
    { key: '3', value: { age: 17, firstName: 'Stepan', lastName: 'Lukov' } },
    { key: '4', value: { age: 59, firstName: 'Ibragim', lastName: 'Lukov' } },
  ])

  await kv.setMany([
    { key: '2', value: { age: 70, firstName: 'Ivan', lastName: 'Sidorov' } },
    { key: '5', value: { age: 33, firstName: 'Dmitry', lastName: 'Nabiulin' } },
  ])

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":[]}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["3","4"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:2', '{"value":{"age":70,"firstName":"Ivan","lastName":"Sidorov"}}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
    ['keyv:$secondary-index:byAge:70', '{"value":["2"]}'],
    ['keyv:$secondary-index:byLastName:Sidorov', '{"value":["2"]}'],
    ['keyv:$secondary-index:byAge:33', '{"value":["5"]}'],
    ['keyv:$secondary-index:byLastName:Nabiulin', '{"value":["5"]}'],
    ['keyv:5', '{"value":{"age":33,"firstName":"Dmitry","lastName":"Nabiulin"}}'],
  ])
})

test('TypeScript generics', async () => {
  type Id = number & { __brand: 'id' }
  const kv = new KeyvSecondary<
    Id,
    { age: number; firstName: string; id: Id; lastName: string },
    'byAge' | 'byLastName'
  >(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
      locker: async cb => cb(), // identity fn, no mutex logic
    }
  )

  await kv.setMany([
    { key: 1 as Id, value: { age: 30, firstName: 'Galina', id: 1 as Id, lastName: 'Ivanova' } },
    { key: 2 as Id, value: { age: 59, firstName: 'Zinaida', id: 2 as Id, lastName: 'Petrovna' } },
    // @ts-expect-error
    { key: 3, value: { age: 17, firstName: 'Stepan', id: 3, lastName: 'Lukov' } },
    { key: 4 as Id, value: { age: 59, firstName: 'Ibragim', id: 4 as Id, lastName: 'Lukov' } },
  ])

  await kv.set(5 as Id, { age: 35, firstName: 'Semen', id: 5 as Id, lastName: 'Zhukov' })
  // @ts-expect-error
  await kv.set(6, { age: 12, firstName: 'Petya', id: 6, lastName: 'Kuznetsov' })

  // @ts-expect-error
  await kv.delete(6)
  await kv.delete(6 as Id)

  await kv.deleteMany([5, 6] as Id[])
  // @ts-expect-error
  await kv.deleteMany([5, 6])

  await kv.get(1 as Id)
  // @ts-expect-error
  await kv.get(1)

  // @ts-expect-error
  await kv.getMany([1, 2, 3])
  await kv.getMany([1, 2, 3] as Id[])

  assert.strictEqual(await kv.has(1 as Id), true)
  // @ts-expect-error
  await kv.has(1)

  // @ts-expect-error
  assert.deepStrictEqual(await kv.hasMany([1, 2, 3, -1]), [true, true, true, false])
  await kv.hasMany([1, 2, 3, -1] as Id[])
})

test('should properly deleteMany', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
        {
          field: 'lastName',
          filter() {
            return true
          },
          name: 'byLastName',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  await kv.deleteMany(['1', '7', '3'])

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":[]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":[]}'],
    ['keyv:$secondary-index:byAge:59', '{"value":["2","4"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:17', '{"value":[]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["4"]}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
  ])
})

test('should support map function in secondary index', async () => {
  const kv = new KeyvSecondary<
    string,
    { age: number; firstName: string; lastName: string },
    'byInitial' | 'byAgeGroup'
  >(
    {},
    {
      indexes: [
        {
          // Index by first letter of firstName using map function
          map: val => val.firstName[0],
          filter: () => true,
          name: 'byInitial',
        },
        {
          // Index by age group using map function
          map: val => (val.age >= 18 ? 'adult' : 'minor'),
          filter: () => true,
          name: 'byAgeGroup',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  // Test byInitial index
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'G'), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'Z'), [
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'S'), [{ age: 17, firstName: 'Stepan', lastName: 'Lukov' }])

  // Test byAgeGroup index
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'adult'), [
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
    { age: 59, firstName: 'Ibragim', lastName: 'Lukov' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'minor'), [
    { age: 17, firstName: 'Stepan', lastName: 'Lukov' },
  ])
})

test('should support map function with filtered indexes', async () => {
  const kv = new KeyvSecondary<
    string,
    { age: number; firstName: string; lastName: string },
    'byInitial' | 'byAgeGroup'
  >(
    {},
    {
      indexes: [
        {
          // Index by first letter of firstName using map function, but only for names starting with 'G' or 'S'
          map: val => val.firstName[0],
          filter: val => val.firstName.startsWith('G') || val.firstName.startsWith('S'),
          name: 'byInitial',
        },
        {
          // Index by age group using map function, but only for adults
          map: val => (val.age >= 18 ? 'adult' : 'minor'),
          filter: val => val.age >= 18,
          name: 'byAgeGroup',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov' })

  // Test filtered byInitial index
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'G'), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'S'), [{ age: 17, firstName: 'Stepan', lastName: 'Lukov' }])
  // Zinaida should not be in index since filter excludes her
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'Z'), [])

  // Test filtered byAgeGroup index
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'adult'), [
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
    { age: 59, firstName: 'Ibragim', lastName: 'Lukov' },
  ])
  // Stepan should not be in index since age filter excludes him
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'minor'), [])
})

test('should support map with complex functions', async () => {
  const kv = new KeyvSecondary<
    string,
    { age: number; firstName: string; lastName: string },
    'byFullNameLength' | 'byNameParts'
  >(
    {},
    {
      indexes: [
        {
          // Index by full name length
          map: val => val.firstName.length + val.lastName.length,
          filter: () => true,
          name: 'byFullNameLength',
        },
        {
          // Index by last name length
          map: val => val.lastName.length,
          filter: () => true,
          name: 'byNameParts',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })

  // Test byFullNameLength index
  assert.deepStrictEqual(await kv.getByIndex('byFullNameLength', 13), [
    // Galina Ivanova = 6 + 7
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byFullNameLength', 15), [
    // Zinaida Petrovna = 7 + 8
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byFullNameLength', 11), [
    // Stepan Lukov = 6 + 5
    { age: 17, firstName: 'Stepan', lastName: 'Lukov' },
  ])

  // Test byNameParts index
  assert.deepStrictEqual(await kv.getByIndex('byNameParts', 7), [
    // Ivanova
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byNameParts', 8), [
    // Petrovna
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byNameParts', 5), [
    // Lukov
    { age: 17, firstName: 'Stepan', lastName: 'Lukov' },
  ])
})

test('should handle null/undefined values in map function', async () => {
  const kv = new KeyvSecondary<
    string,
    { age: number; firstName: string; lastName: string; middleName?: string },
    'byMiddleNameExists'
  >(
    {},
    {
      indexes: [
        {
          // Index by whether middle name exists
          map: val => val.middleName != null,
          filter: () => true,
          name: 'byMiddleNameExists',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 59, firstName: 'Zinaida', lastName: 'Petrovna', middleName: 'Alexeevna' })
  await kv.set('3', { age: 17, firstName: 'Stepan', lastName: 'Lukov' })
  await kv.set('4', { age: 59, firstName: 'Ibragim', lastName: 'Lukov', middleName: 'Ivanovich' })

  // Test index with middle name
  assert.deepStrictEqual(await kv.getByIndex('byMiddleNameExists', true), [
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna', middleName: 'Alexeevna' },
    { age: 59, firstName: 'Ibragim', lastName: 'Lukov', middleName: 'Ivanovich' },
  ])

  // Test index without middle name
  assert.deepStrictEqual(await kv.getByIndex('byMiddleNameExists', false), [
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
    { age: 17, firstName: 'Stepan', lastName: 'Lukov' },
  ])
})

test('should work with setMany and map function', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byInitial'>(
    {},
    {
      indexes: [
        {
          // Index by first letter of firstName using map function
          map: val => val.firstName[0],
          filter: () => true,
          name: 'byInitial',
        },
      ],
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '2', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
    { key: '3', value: { age: 17, firstName: 'Stepan', lastName: 'Lukov' } },
  ])

  // Test that all entries are indexed correctly
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'G'), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'Z'), [
    { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'S'), [{ age: 17, firstName: 'Stepan', lastName: 'Lukov' }])
})

test('should properly update with set when map function changes', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAgeGroup'>(
    {},
    {
      indexes: [
        {
          // Index by age group
          map: val => (val.age >= 18 ? 'adult' : 'minor'),
          filter: () => true,
          name: 'byAgeGroup',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  // Initially an adult
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'adult'), [
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
  ])

  // Update to make it a minor
  await kv.set('1', { age: 15, firstName: 'Galina', lastName: 'Ivanova' })
  // Should no longer be in adult index
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'adult'), [])
  // Should be in minor index
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'minor'), [
    { age: 15, firstName: 'Galina', lastName: 'Ivanova' },
  ])
})

test('should work with multiple indexes that use map function', async () => {
  const kv = new KeyvSecondary<
    string,
    { age: number; firstName: string; lastName: string },
    'byInitial' | 'byAgeGroup'
  >(
    {},
    {
      indexes: [
        {
          // Index by first letter of firstName using map function
          map: val => val.firstName[0],
          filter: () => true,
          name: 'byInitial',
        },
        {
          // Index by age group using map function
          map: val => (val.age >= 18 ? 'adult' : 'minor'),
          filter: () => true,
          name: 'byAgeGroup',
        },
      ],
    }
  )

  await kv.set('1', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  await kv.set('2', { age: 17, firstName: 'Zinaida', lastName: 'Petrovna' })

  // Test both indexes
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'G'), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'adult'), [
    { age: 30, firstName: 'Galina', lastName: 'Ivanova' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byInitial', 'Z'), [
    { age: 17, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
  assert.deepStrictEqual(await kv.getByIndex('byAgeGroup', 'minor'), [
    { age: 17, firstName: 'Zinaida', lastName: 'Petrovna' },
  ])
})

test('should fail when index has no field or map', async () => {
  assert.throws(
    () => {
      new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
        {},
        {
          indexes: [
            {
              filter() {
                return true
              },
              name: 'byAge',
            },
          ],
        }
      )
    },
    {
      message: "On index \"byAge\" need to setup 'field' or 'map'",
    }
  )
})

test('should bypass to native set keys that match $secondary-index pattern', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
      ],
    }
  )

  const result = await kv.set('$secondary-index:byAge:30', { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  assert.strictEqual(result, true)

  assert.deepStrictEqual(await kv.get('$secondary-index:byAge:30'), {
    age: 30,
    firstName: 'Galina',
    lastName: 'Ivanova',
  })
  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:30', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
  ])
})

test('should bypass to native setMany keys that match $secondary-index pattern', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
      ],
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '$secondary-index:byAge:18', value: { age: 30, firstName: 'Zinaida', lastName: 'Petrovna' } },
    { key: '3', value: { age: 17, firstName: 'Stepan', lastName: 'Lukov' } },
  ])

  assert.deepStrictEqual(await kv.get('$secondary-index:byAge:18'), {
    age: 30,
    firstName: 'Zinaida',
    lastName: 'Petrovna',
  })

  assert.deepStrictEqual(await kv.get('1'), { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  assert.deepStrictEqual(await kv.get('3'), { age: 17, firstName: 'Stepan', lastName: 'Lukov' })

  assert.deepStrictEqual(Array.from(kv.store), [
    ['keyv:$secondary-index:byAge:18', '{"value":{"age":30,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:$secondary-index:byAge:30', '{"value":["1"]}'],
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
  ])
})

test('should handle setMany with only native set keys (entitiesToSet empty, entitiesToNativeSet not empty)', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
      ],
    }
  )

  await kv.setMany([
    { key: '$secondary-index:byAge:30', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '$secondary-index:byAge:59', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
  ])

  assert.deepStrictEqual(await kv.get('$secondary-index:byAge:30'), {
    age: 30,
    firstName: 'Galina',
    lastName: 'Ivanova',
  })
  assert.deepStrictEqual(await kv.get('$secondary-index:byAge:59'), {
    age: 59,
    firstName: 'Zinaida',
    lastName: 'Petrovna',
  })
})

test('should handle setMany with only regular keys (entitiesToSet not empty, entitiesToNativeSet empty)', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
      ],
    }
  )

  await kv.setMany([
    { key: '1', value: { age: 30, firstName: 'Galina', lastName: 'Ivanova' } },
    { key: '2', value: { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' } },
  ])

  assert.deepStrictEqual(await kv.get('1'), { age: 30, firstName: 'Galina', lastName: 'Ivanova' })
  assert.deepStrictEqual(await kv.get('2'), { age: 59, firstName: 'Zinaida', lastName: 'Petrovna' })

  assert.deepStrictEqual(await kv.getByIndex('byAge', 30), [{ age: 30, firstName: 'Galina', lastName: 'Ivanova' }])
  assert.deepStrictEqual(await kv.getByIndex('byAge', 59), [{ age: 59, firstName: 'Zinaida', lastName: 'Petrovna' }])
})

test('should handle setMany with no keys (entitiesToSet empty and entitiesToNativeSet empty)', async () => {
  const kv = new KeyvSecondary<string, { age: number; firstName: string; lastName: string }, 'byAge'>(
    {},
    {
      indexes: [
        {
          field: 'age',
          filter() {
            return true
          },
          name: 'byAge',
        },
      ],
    }
  )

  await kv.setMany([])

  assert.deepStrictEqual(Array.from(kv.store), [])
})
