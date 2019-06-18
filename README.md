# duckDb 

MySQL Document Store

Manage Document Data in Mysql

## Install

```
npm install @twilson63/duckdb
```

## Basics

```
const duckdb = require('@twilson63/duckdb')
const connectionInfo = {host: 'localhost', user: 'root', database: 'app'}
const keys = ['type']
const store = 'mythings'
const db = duckdb(connectionInfo, keys)(store)

// create document
const result = db.post({_id: 'greeting-1', type: 'greeting', greeting: 'hello', name: 'world'})
console.log(result)

// get document
const doc = await db.get('greeting-1')

// update document
const result2 = await db.put({_id: 'greeting-1', _rev: doc._rev, type: 'greeting', greeting: 'hello', name: 'moon'})
 
// remove document
const result3 = await db.remove({_id: 'greeting-1', _rev: result2.rev})

// all documents
const docs = await db.allDocs()

```


