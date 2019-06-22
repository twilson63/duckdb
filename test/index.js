const test = require('tape')
const duckdb = require('../')
const mysql = require('mysql')

const info = {
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'test'
}

test('find by type', async t => {
  const db = duckdb(info, ['type'])('twilson63')
  // setup
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      resolve(true)
    })
  })

  const docs = await db.query({ type: 'foo'})

  t.equal(docs.length, 1)
  console.log(docs)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 


})



test('get all docs', async t => {
  const db = duckdb(info, ['type'])('twilson63')
  // setup
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '2', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '2', _rev: '1-1', type: 'foo', hello: 'mars'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '3', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '3', _rev: '1-1', type: 'foo', hello: 'moon'})}, (err, result) => {
      resolve(true)
    })
  })

  const docs = await db.allDocs({ start: '2', end: '3'})

  t.equal(docs.length, 1)
  console.log(docs)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 

})

test('get all docs', async t => {
  const db = duckdb(info, ['type'])('twilson63')
  // setup
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '2', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '2', _rev: '1-1', type: 'foo', hello: 'mars'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '3', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '3', _rev: '1-1', type: 'foo', hello: 'moon'})}, (err, result) => {
      resolve(true)
    })
  })

  const docs = await db.allDocs({ keys: ['1','3']})

  t.equal(docs.length, 2)
  console.log(docs)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 

})


test('get all docs', async t => {
  const db = duckdb(info, ['type'])('twilson63')
  // setup
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '2', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '2', _rev: '1-1', type: 'foo', hello: 'mars'})}, (err, result) => {
      resolve(true)
    })
  })
  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '3', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '3', _rev: '1-1', type: 'foo', hello: 'moon'})}, (err, result) => {
      resolve(true)
    })
  })

  const docs = await db.allDocs()

  t.equal(docs.length, 3)
  console.log(docs)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 

})


test('remove document', async t => {
  const base = duckdb(info, ['type'])
  const db = base('twilson63')
   //setup
  const conn = mysql.createConnection(info)
  conn.connect()

  await new Promise((resolve) => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      if (err) { console.log(err) }
      resolve(true)
    })
  })

  const result = await db.remove({_id: '1', _rev: '1-1' })
  console.log(result)
  t.ok(result.ok)

  const exists = await new Promise(function (resolve, reject) {
    conn.query(`select 1 from twilson63 where _id = ?`, ['1'], (err, results) => {
      if (err) { return reject(err) }
      resolve(results.length === 1)
    })
  })
  t.ok(!exists)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 
})

test('put document no id should create document', async t => {
  const base = duckdb(info, ['type'])
  const db = base('twilson63')
   //setup
  const conn = mysql.createConnection(info)
  conn.connect()

  const result = await db.put({_id: '1', type: 'foo', name: 'World', greeting: 'Hello' })
  t.ok(result.ok)

  const newDoc = await new Promise(function (resolve, reject) {
    conn.query(`select document from twilson63 where _id = ?`, ['1'], (err, results) => {
      if (err) { return reject(err) }
      resolve(JSON.parse(results[0].document))
    })
  })
  t.equal(newDoc._rev, result.rev)
  
  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })

  conn.end()
  db.close(t.end)
 
})

test('put document with conflict', async t => {
  const base = duckdb(info, ['type'])
  const db = base('twilson63')
   //setup
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise(resolve => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      if (err) { console.log(err) }
      resolve(true)
    })
  })
  const result = await db.put({_id: '1', _rev: '2-1', type: 'foo', name: 'World', greeting: 'Hello' })
  t.equal(result.error, 'document conflict')

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })
  
  conn.end()
  db.close(t.end)
 
})

test('put document no conflict', async t => {
  const base = duckdb(info, ['type'])
  const db = base('twilson63')
   //setup
  const conn = mysql.createConnection(info)
  conn.connect()

  await new Promise(resolve => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      if (err) { console.log(err) }
      resolve(true)
    })
  })
  const result = await db.put({_id: '1', _rev: '1-1', type: 'foo', name: 'World', greeting: 'Hello' })
  t.ok(result.ok)

  const newDoc = await new Promise(function (resolve, reject) {
    conn.query(`select document from twilson63 where _id = ?`, ['1'], (err, results) => {
      if (err) { return reject(err) }
      resolve(JSON.parse(results[0].document))
    })
  })
  t.equal(newDoc._rev, result.rev)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })
  conn.end()
  db.close(t.end)
 
})

test('get document', async t => {
  // start init
  const base = duckdb(info,['type'])
  const db = base('twilson63')

   //setup
  const conn = mysql.createConnection(info)
  conn.connect()

  await new Promise(resolve => {
    conn.query('insert into twilson63 set ?', {_id: '1', _rev: '1-1', type: 'foo', document: JSON.stringify({_id: '1', _rev: '1-1', type: 'foo', hello: 'world'})}, (err, result) => {
      if (err) { console.log(err) }
      resolve(true)
    })
  })

 
  const result = await db.get('1')
  console.log(result)
  t.ok(true)

  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })
  conn.end()
  db.close(t.end)
})

test('create document', async t => {
  const base = duckdb(info, ['type'])

  const db = base('twilson63')

  const result = await db.post({
    type: 'test',
    hello: 'world'
  })
  t.ok(result.ok)
  // clean up
  const conn = mysql.createConnection(info)
  conn.connect()
  await new Promise((resolve, reject) => {
    conn.query('delete from twilson63', () => resolve(true))
  })
  conn.end()
  db.close(t.end)
})

