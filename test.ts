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

test('should construct KeyvSecondary correctly with concurrency', async () => {
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

test('should not construct KeyvSecondary correctly with concurrency without locker', async () => {
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
    ['keyv:$secondary-index:byAge:59', '{"value":["4"]}'], // only last id
    ['keyv:$secondary-index:byAge:17', '{"value":["3"]}'],
    ['keyv:$secondary-index:byLastName:Ivanova', '{"value":["1"]}'],
    ['keyv:$secondary-index:byLastName:Petrovna', '{"value":["2"]}'],
    ['keyv:$secondary-index:byLastName:Lukov', '{"value":["4"]}'], // only last id
    ['keyv:1', '{"value":{"age":30,"firstName":"Galina","lastName":"Ivanova"}}'],
    ['keyv:2', '{"value":{"age":59,"firstName":"Zinaida","lastName":"Petrovna"}}'],
    ['keyv:3', '{"value":{"age":17,"firstName":"Stepan","lastName":"Lukov"}}'],
    ['keyv:4', '{"value":{"age":59,"firstName":"Ibragim","lastName":"Lukov"}}'],
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
})
