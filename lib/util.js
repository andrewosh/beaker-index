const SEP = '!'

const streams = new Set()
async function collectStream (stream, drive, opts) {
  const bufs = []
  streams.add(stream)
  setTimeout(() => {
    if (streams.has(stream)) {
      console.log('STREAM STILL PENDING', drive.key.toString('hex'), opts)
    }
  }, 1000 * 60 * 3)
  return new Promise((resolve, reject) => {
    stream.on('end', () => {
      streams.delete(stream)
      process.nextTick(resolve, bufs)
    })
    stream.on('error', err => {
      console.log('DIFF STREAM ERR:', err)
      process.nextTick(resolve, err)
    })
    stream.on('data', d => bufs.push(d))
  })
}

function toKey (...components) {
  return components.join(SEP)
}

function normalizeUrl (url) {
  try {
    let urlp = new URL(url)
    return urlp.toString()
  } catch (e) {
    return url
  }
}

async function dbBatch (db, batch) {
  const b = db.batch()
  for (const entry of batch) {
    if (entry.type === 'put') {
      await b.put(entry.key, entry.value)
    } else if (entry.type === 'del') {
      await b.del(entry.key, entry.value)
    }
  }
  return b.flush()
}

module.exports = {
  collectStream,
  toKey,
  normalizeUrl,
  dbBatch
}