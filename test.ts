import test from 'node:test'
import { KeyvSecondary } from './index.ts'
import assert from 'node:assert'

test('should construct KeyvSecondary', async () => {
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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
  const kv = new KeyvSecondary<{ age: number; firstName: string; lastName: string }, 'byAge' | 'byLastName'>(
    void 0,
    void 0,
    [
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
    ]
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

  const kv = new KeyvSecondary<Person, Indexes>(void 0, void 0, [
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
  ])

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
