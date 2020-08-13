module.exports = class FirehoseIndexer {
  static VERSION = 1

  constructor (db) {
    this.db = db
  }

  async process (user, drive, change) {
    return []
  }
}