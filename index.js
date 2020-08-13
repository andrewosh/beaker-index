const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const hyperdrive = require('hyperdrive')
const HyperBee = require('hyperbee')
const Corestore = require('corestore')
const CorestoreNetworker = require('@corestore/networker')
const registerCoreTimeouts = require('hypercore-networking-timeouts')
const got = require('got')

const TaskQueue = require('./lib/queue')
const FirehoseIndexer = require('./lib/indexers/firehose')
const SubscriptionsIndexer = require('./lib/indexers/subscriptions')
const { dbBatch, toKey, normalizeUrl, collectStream } = require('./lib/util')

const NAMESPACE = 'beaker-index'
const NEW_USER_CHECK_INTERVAL = 1000 * 60 * 2
const CONCURRENT_TASK_LIMIT = 20

module.exports = class BeakerIndexer extends Nanoresource {
  constructor (store, networker, key, opts = {}) {
    super()
    this.store = store
    this.networker = networker
    this.key = key

    // Set in _open.
    this.core = null
    this.db = null
    this.firehose = null
    this.subscriptions = null

    this._queue = new TaskQueue({
      maxConcurrent: CONCURRENT_TASK_LIMIT,
      ...opts
    })
    this._queue.on('task-error', err => this.emit('indexing-error', 'err'))
    this._watchTimer = null
    this._watching = new Set()
    this._watchers = []
    this._indexers = null

    this.indexerVersion = getIndexerVersion()
    this.ready = this.open.bind(this)
  }

  // Nanoresource Methods

  async _open () {
    if (!this.store) this.store = new Corestore('./data', {
      cacheSize: 10000
    })
    this.store = this.store.namespace(NAMESPACE)
    await this.store.ready()

    if (!this.networker) {
      this.networker = new CorestoreNetworker(this.store, {
        // When running on a cloud instance, there can be lots of persistent connections.
        maxPeers: 10000
      })
      registerCoreTimeouts(this.networker, this.store)
    }

    this.core = this.key ? this.store.get(key) : this.store.default()
    this.db = new HyperBee(this.core, {
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    })
    await this.db.ready()

    const announce = this.core.writable
    await this.networker.configure(this.core.discoveryKey, { announce, lookup: !announce })

    this.firehose = new FirehoseIndexer(this.db)
    this.subscriptions = new SubscriptionsIndexer(this.db)
    this._indexers = [this.firehose, this.subscriptions]
  }

  async _close () {
    if (this._watchTimer) clearInterval(this._watchTimer)
    for (const watcher of this._watchers) watcher.destroy()
    await this.networker.close()
    await this.store.close()
  }

  // Indexing-Mode Methods

  _lastVersionKey (url) {
    return toKey('last-version', url)
  }

  async _getLastVersions (url) {
    console.log('GETTING LAST VERSION')
    let versionRecord = await this.db.get(this._lastVersionKey(url))
    if (!versionRecord) return null
    return versionRecord.value
  }

  async _loadDrive (url) {
    const key = urlToKey(url)
    const userDrive = hyperdrive(this.store, key)
    await userDrive.promises.ready()
    this.networker.configure(userDrive.discoveryKey, { announce: false, lookup: true, flush: true })
    return userDrive
  }

  async _getUsersList () {
    try {
      var res = await got('https://userlist.beakerbrowser.com/list.json', {responseType: 'json'})
    } catch (e) {
      console.error(e)
      throw new Error('Failed to fetch users')
    }
    this.emit('fetched-user-list', res.body.users)
    return res.body.users.map(user => ({
      url: normalizeUrl(user.driveUrl),
      title: user.title,
      description: user.description
    }))
  }

  async _indexChanges (user, drive) {
    this.emit('indexing-user', user.url)
    const currentDriveVersion = drive.version
    let lastVersions = await this._getLastVersions(user.url)
    if (lastVersions) {
      const sameDriveVersion = currentDriveVersion === lastVersions.drive
      const sameIndexerVersion = this.indexerVersion === lastVersions.indexer
      // TODO: Support incrementally updating each index individually.
      if (sameDriveVersion && sameIndexerVersion) {
        this.emit('skipping-user', user.url)
        return
      }
    }

    const lastDriveVersion = (lastVersions && lastVersions.drive) || 0
    const diffStream = drive.createDiffStream(lastDriveVersion, '/')

    const changes = await collectStream(diffStream, drive, { lastDriveVersion })
    const batch = []
    for (const change of changes) {
      for (const indexer of this._indexers) {
        const newRecords = await indexer.process(user, drive, change)
        batch.push(...newRecords)
      }
    }
    batch.push({ type: 'put', key: this._lastVersionKey(user.url), value: {
      drive: currentDriveVersion,
      indexer: this.indexerVersion
    }})
    this.emit('indexed-user', user.url, batch)
    return dbBatch(this.db, batch)
  }

  // Public Methods

  startIndexing () {
    const tick = async () => {
      try {
        const users = await this._getUsersList()
        this.emit('fetched-users', users)
        for (const user of users) {
          if (this._watching.has(user.url)) continue
          this._watching.add(user.url)
          this.emit('watching-user', user.url)
          const drive = await this._loadDrive(user.url)
          // Watch the top-level trie only so that we don't get triggered by mounts.
          const watcher = drive.db.trie.watch(() => {
            this.emit('user-changed', user.url)
            this._queue.push(() => this._indexChanges(user, drive))
          })
          this._watchers.push(watcher)
        }
      } catch (err) {
        this.emit('watch-error', err)
      }
    }
    this._watchTimer = setInterval(tick, NEW_USER_CHECK_INTERVAL)
    tick()
  }
}

async function main () {

  while (true) {
    console.log('Indexer tick', (new Date()).toLocaleString())
    try {
      let users = await getUsersList()
      console.log(users.length, 'users')      

      let newDb = JSON.parse(JSON.stringify(currentDb))

      for (let user of users) {
        let userDrive
        try {
          console.log('Indexing', user)
          userDrive = await loadDrive(hclient, user.url)
          
          let userManifest = await readManifest(userDrive)
          if (userManifest) {
            if (user.title && typeof user.title === 'string' && user.title !== userManifest.title) user.title = userManifest.title
            if (user.description && typeof user.description === 'string' && user.description !== userManifest.description) user.description = userManifest.description
          }
          let dbUser = newDb.sources.find(u => u.url === user.url)
          if (dbUser) {
            if (dbUser.title !== user.title) dbUser.title = user.title
            if (dbUser.description !== user.description) dbUser.description = user.description
          } else {
            newDb.sources.push(user)
          }

          await indexLinks(newDb, user, userDrive)
          
          if (!deepEqual(currentDb, newDb)) {
            console.log('Writing new database')
            await writeDb(indexDrive, newDb)
            currentDb = JSON.parse(JSON.stringify(newDb))
          }
        } catch (e) {
          console.log('Failed to index user', e)
        } finally {
          if (userDrive) await userDrive.promises.close()
        }
      }  
      
      // TODO
      // we need to prune sources that have been removed from the userlist
      // which requires changing all the `sourceIndex` values in the links
      // -prf


    } catch (e) {
      console.log('Error during tick', e)
    }
  }
}

async function ensureIndexDriveManifest (indexDrive) {
  const indexDriveManifest = JSON.stringify({
    title: 'Beaker Index',
    description: 'An index generated from Beaker\'s userlist'
  }, null, 2)
  const currentManifest = await indexDrive.promises.readFile('/index.json', 'utf8').catch(e => '')
  if (currentManifest !== indexDriveManifest) {
    await indexDrive.promises.writeFile('/index.json', indexDriveManifest, 'utf8')
  }
}

async function getUsersList () {
  try {
    var res = await got('https://userlist.beakerbrowser.com/list.json', {responseType: 'json'})
  } catch (e) {
    console.error(e)
    throw new Error('Failed to fetch users')
  }
  this.emit('fetched-users', res.body.users)
  return res.body.users.map(user => ({
    url: normalizeUrl(user.driveUrl),
    title: user.title,
    description: user.description
  }))
}

async function readDb (indexDrive) {
  try {
    const str = await indexDrive.promises.readFile('/db.json', 'utf8').catch(e => '')
    const obj = JSON.parse(str)
    if (!obj.sources || !Array.isArray(obj.sources)) throw "invalid"
    if (!obj.links || typeof obj.links !== 'object') throw "invalid"
    return obj
  } catch (e) {
    return {sources: [], links: {}}
  }
}

async function writeDb (indexDrive, db) {
  await indexDrive.promises.writeFile('/db.json', JSON.stringify(db, null, 2), 'utf8')
}

async function loadDrive (networker, url) {
  const key = urlToKey(url)
  const userDrive = hyperdrive(hclient.corestore, key)
  await userDrive.promises.ready()
  await networker.configure(userDrive.discoveryKey, { announce: false, lookup: true, flush: true })
  return userDrive
}

async function readManifest (userDrive) {
  return timeout(10e3, undefined, async () => {
    const str = await userDrive.promises.readFile('/index.json', 'utf8').catch(e => undefined)
    try {
      return JSON.parse(str)
    } catch (e) {
      return undefined
    }
  })
}

async function indexLinks (db, user, userDrive) {
  var sourceIndex = db.sources.findIndex(s => s.url === user.url)

  // clear out existing
  for (let group in db.links) {
    db.links[group] = db.links[group].filter(link => link.sourceIndex !== sourceIndex)
  }

  // pull current
  let linksFolders = await timeout(10e3, [], () => userDrive.promises.readdir('/links').catch(e => ([])))
  for (let folder of linksFolders) {
    let gotos = await timeout(10e3, [], () => userDrive.promises.readdir(`/links/${folder}`, {includeStats: true}).catch(e => ([])))
    for (let goto of gotos.filter(item => item.name.endsWith('.goto'))) {
      if (!goto.stat.metadata.href) continue
      db.links[folder] = db.links[folder] || []
      db.links[folder].push({
        sourceIndex,
        title: goto.stat.metadata.title ? goto.stat.metadata.title.toString('utf8') : goto.name,
        description: goto.stat.metadata.description ? goto.stat.metadata.description.toString('utf8') : undefined,
        href: normalizeUrl(goto.stat.metadata.href.toString('utf8'))
      })
    }
  }
}

function getIndexerVersion () {
  console.log('versions:', SubscriptionsIndexer.VERSION, FirehoseIndexer.VERSION)
  return [
    SubscriptionsIndexer.VERSION,
    FirehoseIndexer.VERSION
  ].join()
}

function urlToKey (url) {
  return Buffer.from(/([0-9a-f]{64})/i.exec(url)[1], 'hex')
}

function deepEqual (x, y) {
  if (x === y) {
    return true;
  }
  else if ((typeof x == "object" && x != null) && (typeof y == "object" && y != null)) {
    if (Object.keys(x).length != Object.keys(y).length) {
      return false;
    }

    for (var prop in x) {
      if (y.hasOwnProperty(prop))
      {  
        if (! deepEqual(x[prop], y[prop]))
          return false;
      }
      else {
        return false;
      }
    }

    return true;
  }
  else {
    return false;
  }
}
