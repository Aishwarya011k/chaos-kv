const http = require('http')
const net = require('net')

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST')

  const url = new URL(req.url, `http://${req.headers.host}`)
  if (url.pathname === '/kill') {
    const port = parseInt(url.searchParams.get('port'))
    const sock = net.createConnection(port, 'localhost', () => {
      sock.write('KILL\n')
    })
    sock.on('data', () => {
      sock.end()
      res.end('killed')
    })
    sock.on('error', (e) => {
      res.statusCode = 500
      res.end('error: ' + e.message)
    })
    return
  }
  res.statusCode = 404
  res.end('not found')
})

server.listen(7000, () => console.log('Bridge running on http://localhost:7000'))
