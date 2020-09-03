const crypto = require('crypto')
const linkExtractor = require('markdown-link-extractor')
const { normalizeUrl, toKey } = require('../util')

module.exports = class BacklinksIndexer {
  static VERSION = 1

  constructor (db) {
    this.db = db
    this._indexers = new Map([
      ['comments/', this._indexBacklinks.bind(this)],
      ['microblog/', this._indexBacklinks.bind(this)],
      ['pages/', this._indexBacklinks.bind(this)]
    ])
  }

  _shouldIndex (name) {
    for (const contentType of this._indexers.keys()) {
      if (name.startsWith(contentType)) return true
    }
    return false
  }

  _index (url, name, stat, drive) {
    for (const contentType of this._indexers.keys()) {
      if (name.startsWith(contentType)) return this._indexers.get(contentType)(url, name, stat, drive)
    }
    return []
  }

  async _indexBacklinks (url, name, stat, drive) {
    const content = await drive.promises.readFile(name, { encoding: 'utf-8' })
    if (!name.endsWith('.md')) return []
    const links = linkExtractor(content)
    return {
      source: url,
      links
    }
  }

  _generateId (url, name, timestamp) {
    const hash = crypto.createHash('sha256')
    hash.update(url)
    hash.update(name)
    hash.update(timestamp)
    return hash.digest('hex')
  }

  async process (user, drive, change) {
    // TODO: Delete backlinks when the source is deleted.
    if (!change.name || change.type !== 'put' || !this._shouldIndex(change.name)) return []
    const stat = change.value

    const entry = await this._index(user.url, change.name, stat, drive)
    if (!entry || !entry.links || !entry.links.length) return []

    return entry.links.map(link => {
      // Create one record from target -> source.
      return {
        type: 'put',
        key: toKey('backlinks', normalizeUrl(link), normalizeUrl(entry.source)),
        value: null
      }
    })
  }

  createBacklinksStream (url, opts = {}) {
    opts = {
      gt: toKey('backlinks', normalizeUrl(url), ''),
      lt: toKey('backlinks', normalizeUrl(url), '~'),
      limit: opts.limit,
      reverse: opts.reverse
    }
    return this.db.createReadStream(opts)
  }
}
