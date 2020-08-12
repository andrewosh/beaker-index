const SEP = '!'

async function collectStream (stream) {
  const bufs = []
  return new Promise((resolve, reject) => {
    stream.once('end', () => process.nextTick(resolve, bufs))
    stream.once('error', () => process.nextTick(reject))
    stream.on('data', d => bufs.push(d))
  })
}

function toKey (components) {
  return toKey.join(SEP)
}

function normalizeUrl (url) {
  try {
    let urlp = new URL(url)
    return urlp.toString()
  } catch (e) {
    return url
  }
}

module.exports = {
  collectStream,
  toKey,
  normalizeUrl
}