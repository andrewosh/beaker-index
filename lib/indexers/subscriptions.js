const { collectStream, normalizeUrl, toKey } = require('../util')

module.exports = class SubscriptionsIndexer {
  static VERSION = 1

  constructor (db) {
    this.db = db
    this._targetRe = /.*!(.*)$/
  }

  _getSubscriptionKeys (fromUrl, toUrl) {
    fromUrl = encodeURI(normalizeUrl(fromUrl))
    toUrl = encodeURI(normalizeUrl(toUrl))
    return {
      forward: toKey('subscription', 'forward', fromUrl, toUrl),
      backward: toKey('subscription', 'backward', toUrl, fromUrl)
    }
  }

  async process (user, drive, change) {
    if (!change.name || !change.name.startsWith('subscriptions')) return []
    let target = stat.metadata.href
    if (typeof target !== 'string') target = target.toString('utf-8')
    const keys = this._getSubscriptionKeys(user.url, target)
    return [
      {
        type: change.type,
        key: keys.forward,
        value: {}
      },
      {
        type: change.type,
        key: keys.backward,
        value: {}
      }
    ]
  }

  async getSubscribers (url) {
    url = encodeURI(normalizeUrl(url))
    const subscriptions = await collectStream(this.db.createReadStream({
      gt: this._getSubscriptionKeys('', url).backward,
      lt: this._getSubscriptionKeys('~', url).backward
    }))
    return subscriptions.map(({ key }) => decodeURI(this._targetRe.exec(key)[1]))
  }

  async getSubscriptions (url) {
    url = encodeURI(normalizeUrl(url))
    const subscriptions = await collectStream(this.db.createReadStream({
      gt: this._getSubscriptionKeys(url, '').forward,
      lt: this._getSubscriptionKeys(url, '~').forward
    }))
    return subscriptions.map(({ key }) => decodeURI(this._targetRe.exec(key)[1]))
  }
}