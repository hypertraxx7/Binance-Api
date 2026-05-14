const bf = require('./binance-flow')
await bf.start()
const snap = bf.snapshot()  // retorna arrays estruturados de spot e futures
