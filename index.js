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
const CONCURRENT_TASK_LIMIT = 40

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
    this._queue.on('task-error', err => this.emit('indexing-error', err))
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
      cacheSize: 10000,
      ifAvailable: true
    })
    this.store = this.store.namespace(NAMESPACE)
    await this.store.ready()

    if (!this.networker) {
      this.networker = new CorestoreNetworker(this.store, {
        // When running on a cloud instance, there can be lots of persistent connections.
        maxPeers: 10000
      })
      await this.networker.listen()
      registerCoreTimeouts(this.networker, this.store)
    }

    this.core = this.key ? this.store.get(this.key) : this.store.default()
    this.db = new HyperBee(this.core, {
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    })
    await this.db.ready()

    const announce = this.core.writable
    await this.networker.configure(this.core.discoveryKey, { announce, lookup: !announce })
    if (announce) {
      const dht = this.networker.swarm.network.discovery.dht
      dht.concurrency = 51
      dht.concurrencyRPS = 100
    }

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

    /*
  async _getUsersList () {
    let keys = (await fs.readFile('./output.txt', { encoding: 'utf8' })).split('\n')
    keys = keys.filter(k => !!k)
    return keys.map(k => ({
      url: 'hyper://' + k,
      title: 'blah',
      description: 'blah'
    }))
  }
  */

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
        return null
      }
    }

    const lastDriveVersion = (lastVersions && lastVersions.drive) || 0
    const diffStream = drive.createDiffStream(lastDriveVersion, '/', { noMounts: true })

    const batch = []
    var changes = null
    try {
      changes = await collectStream(diffStream, drive, { lastDriveVersion })
    } catch (err) {
      // If the diff stream could not complete. Do not proceed.
      this.emit('indexing-error', err, user.url)
    }
    if (!changes) return null
    for (const change of changes) {
      for (const indexer of this._indexers) {
        var newRecords = []
        try {
          newRecords = await indexer.process(user, drive, change)
        } catch (err) {
          this.emit('indexing-error', err, user.url)
        }
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

function getIndexerVersion () {
  return [
    SubscriptionsIndexer.VERSION,
    FirehoseIndexer.VERSION
  ].join()
}

function urlToKey (url) {
  return Buffer.from(/([0-9a-f]{64})/i.exec(url)[1], 'hex')
}
