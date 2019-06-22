const mysql = require('mysql')
const uuid = require('uuid')
const crypto = require('crypto')
const { map, compose, join, prop, head, split, omit, inc, values, keys } = require('ramda')

const parseJSON = JSON.parse.bind(JSON)
const sha256 = s => crypto.createHash('sha256').update(s).digest('hex')


module.exports = (info, keys) => {
  const db = mysql.createPool(info)

  return function (table) {
    db.query(createTable(table, keys), err => {
      if (err) { console.log(err) }
    })
    const doIndex = createIndex(table)
    keys.forEach(k => db.query(doIndex(k), noop))

    return {
      allDocs,
      post,
      put,
      get,
      remove,
      query,
      //bulkDocs
      close
    }

    function query(whereValues={}) {
      return new Promise(function (resolve, reject) {
        let whereClause = keys(whereValues).map(k => db.escapeId(k) + ' = ?').join(' and ')
        let query = `select document from ${table} where ${whereClause}`

        db.query(query, values(whereValues), (err, results) => {
          if (err) { return reject(err) }
          resolve(map(compose(parseJSON, prop('document')), results))
        }) 
      })
    }

    function allDocs(options={limit: 20}) {
      return new Promise(function (resolve, reject) {
        function argufy(arr) {
          return compose(
            join(','),
            map(v => `'${v}'`)
          )(arr)
        }
        // get by keys
        // get by range - start, end
        // get by limit 
        //
        let query = `select document from ${table}`
        if (options.keys) {
          query += ` where _id in (${argufy(options.keys)})`
        } 
        if (options.start && options.end) {
          query += ` where _id >= '${options.start}' and _id < '${options.end}'`
        }

        query += ` limit ${options.limit || 20}`
        db.query(query, (err, results) => {
          if (err) { return reject(err) }
          resolve(map(compose(parseJSON, prop('document')), results))
        })
      })

    }

    function remove(doc) {
      return new Promise(function(resolve, reject) {
        db.query(`select _rev from ${table} where _id = ?`, [doc._id], (err, results) => {
          if (err) { return reject(err) }
          if (results.length === 0) { return resolve({error: 'document not found'}) }
          if (results[0]._rev !== doc._rev) { return resolve({error: 'rev does not match'}) }
          db.query(`delete from ${table} where _id = ? and _rev = ?`, [doc._id, doc._rev], (err, result) => {
            if (err) { return reject(err) }
            resolve({ ok: true, id: doc._id, rev: doc._rev})
          })
        })
      })
    }

    function put(doc) {
      return new Promise(function(resolve,reject) {
        db.query(`select _rev from ${table} where _id = ?`,
          [doc._id], (err, results) => {
            if (err) { return reject(err) }
            // if no document and id specified add doc
            // if document but rev does not match reject
            if (results.length === 0) { return resolve(post(doc))  }
            const rev = results[0]._rev
            if (rev !== doc._rev) { return resolve({error: 'document conflict'}) }
            // if document and rev match replace
            doc._rev = buildRev(doc)
            const values = {
              _id: doc._id,
              _rev: doc._rev,
              document: JSON.stringify(doc)
            }
            keys.forEach(k => values[k] = doc[k])
            
            db.query(`update ${table} set ? where _id = ?`, [values,doc._id], (err, result) => {
              if (err) { return reject(err) }
              resolve({
                ok: true,
                id: doc._id,
                rev: doc._rev
              })
            })

          })

      })
    }

    function get(id) {
      return new Promise(function (resolve, reject) {
        db.query(`select document from ${table} where _id = ?`, [id], (err, results) => {
          if (err) { return reject(err) }
          if (results.length === 0) { return resolve({error: 'not_found'}) }
          resolve(parseJSON(results[0].document))
        })
      })
    }

    function post(doc) {
      return new Promise(function(resolve, reject) {
        const hash = sha256(JSON.stringify(doc))
        doc._id = doc._id || uuid.v4()
        doc._rev = '1-' + hash
        let values = {
          _id: doc._id,
          _rev: doc._rev,
          document: JSON.stringify(doc)
        }
        keys.forEach(k => values[k] = doc[k])
        db.query(`INSERT INTO ${table} SET ?`, values, (err, result) => {
          if (err) { return reject(err) }
          resolve({
            ok: true,
            id: doc._id,
            rev: doc._rev
          })
        })
      })
    }

    function close(cb=noop) {
      db.end(cb)
    }

    function createTable(table, keys) {
      return `
 CREATE TABLE IF NOT EXISTS ${table} (
   _id VARCHAR(191) NOT NULL UNIQUE,
   _rev VARCHAR(255) NOT NULL,
   document LONGTEXT NOT NULL,
   ${keys.map(k => `${k} VARCHAR(255)`).join(', ')},
   primary key(_id)
 )
 `
    }

    function createIndex(table) {
      return function (key) {
        return `CREATE INDEX idx_${key} ON ${table} (${key})`
      }
    }
    
    function buildRev(doc) {
      const sha = sha256(JSON.stringify(omit(['_id','_rev'], doc)))
      const version = compose(
        inc,
        head,
        split('-')
      )(doc._rev)
      return `${version}-${sha}`
    }

    function noop() {
      return null
    }
  }

}
