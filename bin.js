const repl = require('repl')
const BeakerIndexer = require('.')

start()
async function start () {
  const indexer = new BeakerIndexer()

  process.on('SIGINT', cleanup)
  process.on('SIGTERM', cleanup)

  indexer.on('watching-user', url => console.log(`Watching User: ${url}`))
  indexer.on('user-changed', url => console.log(` ** User Changed: ${url}`))
  indexer.on('indexing-user', url => console.log(` ** Indexing User: ${url}`))
  indexer.on('skipping-user', url => console.log(` ** Skipping User: ${url}`))
  indexer.on('indexed-user', (url, batch) => console.log(` ** Indexed User: ${url}, Batch: ${batch}`))
  indexer.on('watch-error', err => console.error(`Watch Errored: ${err}`))
  indexer.on('indexing-error', (err, url) => {
    console.error(`Could Not Index ${url}: ${err}`)
    console.error('ERR:', err)
  })
  indexer.on('fetched-users', users => console.log(` ### Fetched ${users.length} Users.`))

  console.log('Starting Beaker indexer...')
  await indexer.ready()

  const cmd = process.argv[2]
  if (!cmd || cmd === 'start') {
    console.log('Indexing into: ', indexer.core.key.toString('hex'))
    indexer.startIndexing()
    setInterval(() => {
      console.log()
      console.log(' !!! ')
      console.log('Task Queue Length: ', indexer._queue.queue.length)
      console.log('Running Tasks: ', indexer._queue._running)
      console.log()
    }, 2000)
  } else if (cmd === 'repl') {
    console.log('Launching REPL...')
    const r = repl.start({
      useGlobal: true
    })
    r.context.indexer = indexer
  }

  async function cleanup () {
    try {
      console.log('Waiting for indexer to close...')
      // await indexer.close()
    } catch (err) {
      console.error('Close Errored:', err)
      process.exit(1)
    }
    process.exit(0)
  }
}
