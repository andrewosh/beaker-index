const lexint = require('lexicographic-integer')
const pumpify = require('pumpify')
const { normalizeUrl, toKey } = require('../util')
const { Transform } = require('streamx')

module.exports = class FirehoseIndexer {
  static VERSION = 1

  constructor (db) {
    this.db = db
    this._indexers = new Map([
      ['comments', this._indexComment.bind(this)],
      ['microblog', this._indexMicroblog.bind(this)],
      ['pages', this._indexPage.bind(this)],
      ['bookmarks', this._indexBookmark.bind(this)]
    ])
  }

  _shouldIndex (name) {
    for (const contentType of this._indexers.keys()) {
      if (name.startsWith(contentType)) return true
    }
    return false
  }

  _index (name, stat, drive) {
    for (const contentType of this._indexers.keys()) {
      if (name.startsWith(contentType)) return this._indexers.get(contentType)(name, stat, drive)
    }
    return []
  }

  async _indexComment (name, stat, drive) {
    const content = await drive.promises.readFile(name)
    return {
      type: 'comment',
      name,
      subject: stat.metadata['beaker/subject'],
      ctime: stat.ctime,
      content
    }
  }

  async _indexBookmark (name, stat, drive) {
    return {
      type: 'bookmark',
      name,
      title: stat.metadata['beaker/title'] || stat.metadata['title'],
      href: stat.metadata['beaker/href'] || stat.metadata['href'],
    }
  }

  async _indexPage (name, stat, drive) {
    return {
      type: 'page',
      name,
      title: stat.metadata['beaker/title'] || stat.metadata['title']
    }
  }
  
  async _indexMicroblog (name, stat, drive) {
    const content = await drive.promises.readFile(name)
    return {
      type: 'microblog',
      name,
      content
    }
  }

  async process (user, drive, change) {
    if (!change.name || !this._shouldIndex(change.name)) return []
    // Since each change is batched independently, this will be unique.
    const recordId = this.db.core.length
    const stat = change.value

    let timestamp = stat.ctime ? Math.min(stat.ctime, Date.now()) : Date.now()
    timestamp = lexint.pack(timestamp, 'hex') + '-' + lexint.pack(recordId, 'hex')

    const recordKey = toKey('records', recordId)
    const firehoseKey = toKey('firehose', timestamp)
    const userKey = toKey('users', encodeURI(normalizeUrl(user.url)), timestamp)

    const entry = await this._index(change.name, stat, drive)

    const records = []
    if (change.type === 'put') {
      // Store the content once, keyed by the record ID.
      records.push({
        type: 'put',
        key: recordKey,
        value: entry
      })
    }
    records.push(...[
      // This is the global firehose record.
      {
        type: change.type,
        key: firehoseKey,
        value: recordId
      },
      // This is the user-specific firehose record.
      {
        type: change.type,
        key: userKey,
        value: recordId
      }
    ])

    return records
  }

  _createRecordFetcher () {
    return new Transform({
      async transform ({ key, value: recordID }, cb) {
        try {
          const recordNode = await this.db.get('records/' + recordID)
          return process.nextTick(cb, null, recordNode && recordNode.value)
        } catch (err) {
          return process.nextTick(cb, err)
        }
      }
    })
  }

  createFirehoseStream (opts = {}) {
    const recordFetcher = this._createRecordFetcher()
    const opts = {
      gt: toKey('firehose', ''),
      lt: toKey('firehose', '~'),
      limit: opts.limit,
      reverse: opts.reverse
    }
    return pumpify.obj(
      this.db.createReadStream(opts),
      this._createRecordFetcher()
    )
  }

  createUserStream (url, opts = {}) {
    url = encodeURI(normalizeUrl(url))
    const recordFetcher = this._createRecordFetcher()
    const opts = {
      gt: toKey('users', url, ''),
      lt: toKey('users', url, '~'),
      limit: opts.limit,
      reverse: opts.reverse
    }
    return pumpify.obj(
      this.db.createReadStream(opts),
      this._createRecordFetcher()
    )
  }
}