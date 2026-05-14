/**
 * script.js
 * ────────────────────────────────────────────────────────────
 * Recolha em tempo real da Binance via WebSocket
 *   • Top 25 Spot USDC
 *   • Top 25 Futures Perp USDT
 *
 * Mostra por símbolo:
 *   • Volume compradores vs vendedores (acumulado na sessão)
 *   • Quem está a movimentar mais o mercado
 *   • Pressão de compra/venda em tempo real
 *
 * Dependências:
 *   npm install ws node-fetch
 *
 * Execução:
 *   node binance-flow.js
 * ────────────────────────────────────────────────────────────
 */

'use strict'

const WebSocket = require('ws')
const https     = require('https')

// ─── CONFIGURAÇÃO ────────────────────────────────────────────

const CFG = {
  TOP_N          : 25,
  REST_TIMEOUT   : 8_000,
  REFRESH_SECS   : 30,        // recarregar ranking de volume a cada N segundos
  PRINT_SECS     : 5,         // intervalo de output no terminal
  WINDOW_MS      : 60_000,    // janela deslizante de trades (1 minuto)
  RECONNECT_MS   : 4_000,

  SPOT_REST  : 'https://api.binance.com/api/v3/ticker/24hr',
  FUT_REST   : 'https://fapi.binance.com/fapi/v1/ticker/24hr',

  SPOT_WS    : 'wss://stream.binance.com:9443/stream',
  FUT_WS     : 'wss://fstream.binance.com/stream',

  EXCL       : ['UP','DOWN','BULL','BEAR','3L','3S','LEVERAGE','HEDGE'],
}

// ─── ANSI CORES ──────────────────────────────────────────────

const C = {
  rst  : '\x1b[0m',
  dim  : '\x1b[2m',
  bold : '\x1b[1m',
  grn  : '\x1b[32m',
  red  : '\x1b[31m',
  ylw  : '\x1b[33m',
  cyn  : '\x1b[36m',
  blu  : '\x1b[34m',
  mag  : '\x1b[35m',
  wht  : '\x1b[97m',
  bgGrn: '\x1b[42m',
  bgRed: '\x1b[41m',
  bgYlw: '\x1b[43m',
}

// ─── ESTADO GLOBAL ───────────────────────────────────────────

/**
 * Estrutura por símbolo:
 * {
 *   symbol      : string,
 *   market      : 'spot' | 'futures',
 *   price       : number,
 *   prevPrice   : number,
 *   change24h   : number,
 *   quoteVol24h : number,           // volume 24h em USDC/USDT (REST)
 *   trades      : Array<{ts, qty, quoteQty, isBuy}>,  // janela deslizante
 *   buyQty      : number,           // acumulado sessão (base asset)
 *   sellQty     : number,
 *   buyQuote    : number,           // acumulado sessão (USDC/USDT)
 *   sellQuote   : number,
 *   tradeCount  : number,
 *   lastTrade   : number,           // timestamp ms
 * }
 */
const STATE = {
  spot    : new Map(),   // symbol → dados
  futures : new Map(),
  spotSyms   : [],       // lista ordenada top 25
  futSyms    : [],
  ws      : { spot: null, futures: null },
  running : false,
}

// ─── UTILITÁRIOS ─────────────────────────────────────────────

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: CFG.REST_TIMEOUT }, res => {
      let raw = ''
      res.on('data', c => raw += c)
      res.on('end', () => {
        try { resolve(JSON.parse(raw)) }
        catch (e) { reject(e) }
      })
    })
    req.on('error', reject)
    req.on('timeout', () => { req.destroy(); reject(new Error('REST timeout')) })
  })
}

function isExcluded(sym) {
  return CFG.EXCL.some(e => sym.includes(e))
}

function fmtNum(n, dec = 2) {
  if (n === null || n === undefined || isNaN(n)) return '—'
  return n.toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec })
}

function fmtVol(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toFixed(2)
}

function fmtPrice(n) {
  if (!n) return '—'
  if (n >= 10000)  return fmtNum(n, 2)
  if (n >= 100)    return fmtNum(n, 3)
  if (n >= 1)      return fmtNum(n, 4)
  if (n >= 0.01)   return fmtNum(n, 5)
  return fmtNum(n, 6)
}

function pad(str, len, right = false) {
  const s = String(str)
  if (right) return s.slice(0, len).padEnd(len)
  return s.slice(0, len).padStart(len)
}

function bar(ratio, width = 16) {
  // ratio: 0–100, preenche de compradores (esquerda) para vendedores (direita)
  const filled = Math.round((ratio / 100) * width)
  const buy  = '█'.repeat(filled)
  const sell = '░'.repeat(width - filled)
  return C.grn + buy + C.red + sell + C.rst
}

// ─── COLHER DADOS REST ───────────────────────────────────────

async function fetchTopSpotUSDC() {
  log(C.dim + '  → Buscando top ' + CFG.TOP_N + ' Spot USDC...' + C.rst)
  const tickers = await httpsGet(CFG.SPOT_REST)
  const filtered = tickers
    .filter(t => t.symbol.endsWith('USDC') && !isExcluded(t.symbol))
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
    .slice(0, CFG.TOP_N)

  STATE.spotSyms = filtered.map(t => t.symbol)

  filtered.forEach((t, i) => {
    const existing = STATE.spot.get(t.symbol) || {}
    STATE.spot.set(t.symbol, {
      ...existing,
      symbol      : t.symbol,
      market      : 'spot',
      rank        : i + 1,
      price       : parseFloat(t.lastPrice),
      prevPrice   : existing.price || parseFloat(t.lastPrice),
      change24h   : parseFloat(t.priceChangePercent),
      quoteVol24h : parseFloat(t.quoteVolume),
      buyQty      : existing.buyQty    || 0,
      sellQty     : existing.sellQty   || 0,
      buyQuote    : existing.buyQuote  || 0,
      sellQuote   : existing.sellQuote || 0,
      tradeCount  : existing.tradeCount || 0,
      trades      : existing.trades    || [],
      lastTrade   : existing.lastTrade || 0,
    })
  })
  log(C.grn + '  ✓ ' + STATE.spotSyms.length + ' pares Spot USDC carregados' + C.rst)
}

async function fetchTopFuturesUSDT() {
  log(C.dim + '  → Buscando top ' + CFG.TOP_N + ' Futures USDT...' + C.rst)
  const tickers = await httpsGet(CFG.FUT_REST)
  const filtered = tickers
    .filter(t => t.symbol.endsWith('USDT') && !isExcluded(t.symbol))
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
    .slice(0, CFG.TOP_N)

  STATE.futSyms = filtered.map(t => t.symbol)

  filtered.forEach((t, i) => {
    const existing = STATE.futures.get(t.symbol) || {}
    STATE.futures.set(t.symbol, {
      ...existing,
      symbol      : t.symbol,
      market      : 'futures',
      rank        : i + 1,
      price       : parseFloat(t.lastPrice),
      prevPrice   : existing.price || parseFloat(t.lastPrice),
      change24h   : parseFloat(t.priceChangePercent),
      quoteVol24h : parseFloat(t.quoteVolume),
      buyQty      : existing.buyQty    || 0,
      sellQty     : existing.sellQty   || 0,
      buyQuote    : existing.buyQuote  || 0,
      sellQuote   : existing.sellQuote || 0,
      tradeCount  : existing.tradeCount || 0,
      trades      : existing.trades    || [],
      lastTrade   : existing.lastTrade || 0,
    })
  })
  log(C.grn + '  ✓ ' + STATE.futSyms.length + ' pares Futures USDT carregados' + C.rst)
}

// ─── PROCESSAR TRADES ────────────────────────────────────────

/**
 * Cada mensagem aggTrade da Binance:
 *   s  = symbol
 *   p  = price (string)
 *   q  = quantity (string)
 *   m  = isBuyerMaker
 *        true  → vendedor foi agressivo (SELL taker)
 *        false → comprador foi agressivo (BUY taker)
 *   T  = timestamp ms
 */
function processTrade(map, data) {
  const sym = data.s
  const entry = map.get(sym)
  if (!entry) return

  const price    = parseFloat(data.p)
  const qty      = parseFloat(data.q)
  const quoteQty = price * qty
  const isBuy    = !data.m          // m=false → BUY agressivo
  const ts       = data.T

  // Acumulado total da sessão
  if (isBuy) {
    entry.buyQty   += qty
    entry.buyQuote += quoteQty
  } else {
    entry.sellQty   += qty
    entry.sellQuote += quoteQty
  }
  entry.tradeCount++
  entry.prevPrice = entry.price
  entry.price     = price
  entry.lastTrade = ts

  // Janela deslizante (último minuto)
  entry.trades.push({ ts, qty, quoteQty, isBuy })
  const cutoff = ts - CFG.WINDOW_MS
  while (entry.trades.length > 0 && entry.trades[0].ts < cutoff) {
    entry.trades.shift()
  }
}

// ─── CALCULAR MÉTRICAS ───────────────────────────────────────

function computeMetrics(entry) {
  const totalQuote = entry.buyQuote + entry.sellQuote
  const buyRatio   = totalQuote > 0 ? (entry.buyQuote / totalQuote) * 100 : 50

  // Janela deslizante (último minuto)
  let winBuy = 0, winSell = 0
  for (const t of entry.trades) {
    if (t.isBuy) winBuy  += t.quoteQty
    else         winSell += t.quoteQty
  }
  const winTotal    = winBuy + winSell
  const winBuyRatio = winTotal > 0 ? (winBuy / winTotal) * 100 : 50
  const winDelta    = winBuy - winSell          // positivo = compradores dominam

  // Pressão: índice -100 a +100
  // Combina ratio de sessão (40%) + ratio janela (60%)
  const pressure = ((buyRatio - 50) * 0.4 + (winBuyRatio - 50) * 0.6) * 2

  // Classificação
  let signal, signalColor
  if      (pressure >=  60) { signal = '🔥🔥 COMPRA EXTREMA';  signalColor = C.bgGrn + C.wht }
  else if (pressure >=  30) { signal = '🔥  COMPRA FORTE   ';  signalColor = C.grn + C.bold }
  else if (pressure >=  10) { signal = '↑   COMPRADORES    ';  signalColor = C.grn }
  else if (pressure >=  -10){ signal = '⚖   EQUILÍBRIO     ';  signalColor = C.ylw }
  else if (pressure >= -30) { signal = '↓   VENDEDORES     ';  signalColor = C.red }
  else if (pressure >= -60) { signal = '📉  VENDA FORTE    ';  signalColor = C.red + C.bold }
  else                       { signal = '💀  VENDA EXTREMA  ';  signalColor = C.bgRed + C.wht }

  return {
    buyRatio,
    winBuyRatio,
    winDelta,
    winBuy,
    winSell,
    pressure,
    signal,
    signalColor,
    totalQuote,
    totalTrades: entry.tradeCount,
  }
}

// ─── WEBSOCKET ───────────────────────────────────────────────

function buildStreams(symbols, type) {
  // aggTrade + miniTicker por símbolo
  return symbols.map(s => s.toLowerCase() + '@aggTrade').join('/')
}

function connectSpotWS() {
  if (!STATE.spotSyms.length) return
  const streams = buildStreams(STATE.spotSyms, 'spot')
  const url     = CFG.SPOT_WS + '?streams=' + streams

  log(C.dim + '  → WS Spot conectando (' + STATE.spotSyms.length + ' streams)...' + C.rst)
  const ws = new WebSocket(url)
  STATE.ws.spot = ws

  ws.on('open',    () => log(C.grn + '  ✓ WS Spot USDC conectado' + C.rst))
  ws.on('message', raw => {
    try {
      const msg  = JSON.parse(raw)
      const data = msg.data || msg
      if (data.e === 'aggTrade') processTrade(STATE.spot, data)
    } catch (_) {}
  })
  ws.on('error',   err => log(C.red + '  ✗ WS Spot erro: ' + err.message + C.rst))
  ws.on('close',   () => {
    log(C.ylw + '  ↺ WS Spot fechado — reconectando em ' + CFG.RECONNECT_MS/1000 + 's...' + C.rst)
    setTimeout(connectSpotWS, CFG.RECONNECT_MS)
  })
}

function connectFuturesWS() {
  if (!STATE.futSyms.length) return
  const streams = buildStreams(STATE.futSyms, 'futures')
  const url     = CFG.FUT_WS + '?streams=' + streams

  log(C.dim + '  → WS Futures conectando (' + STATE.futSyms.length + ' streams)...' + C.rst)
  const ws = new WebSocket(url)
  STATE.ws.futures = ws

  ws.on('open',    () => log(C.grn + '  ✓ WS Futures USDT conectado' + C.rst))
  ws.on('message', raw => {
    try {
      const msg  = JSON.parse(raw)
      const data = msg.data || msg
      if (data.e === 'aggTrade') processTrade(STATE.futures, data)
    } catch (_) {}
  })
  ws.on('error',   err => log(C.red + '  ✗ WS Futures erro: ' + err.message + C.rst))
  ws.on('close',   () => {
    log(C.ylw + '  ↺ WS Futures fechado — reconectando em ' + CFG.RECONNECT_MS/1000 + 's...' + C.rst)
    setTimeout(connectFuturesWS, CFG.RECONNECT_MS)
  })
}

// Fecha e reconecta (após refresh de ranking)
function reconnectWS() {
  ;['spot','futures'].forEach(k => {
    if (STATE.ws[k]) { try { STATE.ws[k].close() } catch (_) {} }
  })
  setTimeout(() => { connectSpotWS(); connectFuturesWS() }, 500)
}

// ─── OUTPUT ──────────────────────────────────────────────────

function log(...args) { process.stdout.write(args.join(' ') + '\n') }

function printHeader(label, quote) {
  const title  = ` ${label} (${quote}) `
  const border = '═'.repeat(Math.max(0, (106 - title.length) / 2))
  log('\n' + C.cyn + C.bold + border + title + border + C.rst)
  log(
    C.dim +
    pad('#',   3)          + ' ' +
    pad('SÍMBOLO',  10, true) + ' ' +
    pad('PREÇO',     12)     + ' ' +
    pad('24H%',       7)     + ' ' +
    pad('VOL 24H',   10)     + ' ' +
    pad('BUY (sessão)',  13) + ' ' +
    pad('SELL (sessão)', 13) + ' ' +
    pad('RATIO',      6)     + ' ' +
    pad('BARRA',      18, true) + ' ' +
    pad('1MIN DELTA',  13)   + ' ' +
    pad('PRESSÃO',    8)     + ' ' +
    'SINAL' +
    C.rst
  )
  log(C.dim + '─'.repeat(107) + C.rst)
}

function printRow(entry, m) {
  const pChg = entry.price >= entry.prevPrice ? C.grn : C.red
  const cClr = entry.change24h >= 0 ? C.grn : C.red
  const pctStr = (entry.change24h >= 0 ? '+' : '') + entry.change24h.toFixed(2) + '%'

  // Ratio sessão
  const totalQuote = entry.buyQuote + entry.sellQuote
  const buyRatio   = totalQuote > 0 ? (entry.buyQuote / totalQuote * 100) : 50

  // Delta janela 1 min
  const deltaSign  = m.winDelta >= 0 ? C.grn : C.red
  const deltaStr   = (m.winDelta >= 0 ? '+' : '') + '$' + fmtVol(Math.abs(m.winDelta))

  log(
    pad(entry.rank, 3)                                             + ' ' +
    C.wht + C.bold + pad(entry.symbol, 10, true) + C.rst          + ' ' +
    pChg + pad(fmtPrice(entry.price), 12) + C.rst                 + ' ' +
    cClr + pad(pctStr, 7) + C.rst                                 + ' ' +
    C.dim + pad('$' + fmtVol(entry.quoteVol24h), 10) + C.rst     + ' ' +
    C.grn + pad('$' + fmtVol(entry.buyQuote), 13) + C.rst        + ' ' +
    C.red + pad('$' + fmtVol(entry.sellQuote), 13) + C.rst       + ' ' +
    (buyRatio >= 50 ? C.grn : C.red) + pad(buyRatio.toFixed(1) + '%', 6) + C.rst + ' ' +
    bar(buyRatio, 16) + ' ' +
    deltaSign + pad(deltaStr, 13) + C.rst                         + ' ' +
    pressureStr(m.pressure)                                        + ' ' +
    m.signalColor + m.signal.trim() + C.rst
  )
}

function pressureStr(p) {
  const v   = Math.round(Math.abs(p))
  const clr = p >= 0 ? C.grn : C.red
  const sig = p >= 0 ? '+' : '-'
  return clr + pad(sig + v, 8) + C.rst
}

function printSummary(entries, market) {
  if (!entries.length) return
  const totalBuy  = entries.reduce((s, e) => s + e.buyQuote, 0)
  const totalSell = entries.reduce((s, e) => s + e.sellQuote, 0)
  const total     = totalBuy + totalSell
  const ratio     = total > 0 ? totalBuy / total * 100 : 50
  const dom       = ratio >= 50
    ? C.grn + 'COMPRADORES DOMINAM  (+' + fmtVol(totalBuy - totalSell) + ' USDC/USDT)' + C.rst
    : C.red + 'VENDEDORES DOMINAM   (-' + fmtVol(totalSell - totalBuy) + ' USDC/USDT)' + C.rst
  log(C.dim + '─'.repeat(107) + C.rst)
  log(
    C.dim + '  TOTAL ' + market.toUpperCase() + ': ' + C.rst +
    C.grn + 'Compra $' + fmtVol(totalBuy) + C.rst + ' vs ' +
    C.red + 'Venda $' + fmtVol(totalSell) + C.rst + '  │  ' +
    'Ratio: ' + (ratio >= 50 ? C.grn : C.red) + ratio.toFixed(1) + '%' + C.rst + '  │  ' + dom
  )
}

function printTopMover(entries, label) {
  if (!entries.length) return

  // maior pressure absoluta
  const withM = entries.map(e => ({ e, m: computeMetrics(e) }))
  const top   = withM.reduce((best, cur) =>
    Math.abs(cur.m.pressure) > Math.abs(best.m.pressure) ? cur : best
  )
  // maior delta 1 min
  const topD  = withM.reduce((best, cur) =>
    Math.abs(cur.m.winDelta) > Math.abs(best.m.winDelta) ? cur : best
  )

  const pressColor = top.m.pressure >= 0 ? C.grn : C.red
  const dColor     = topD.m.winDelta >= 0 ? C.grn : C.red

  log(
    C.ylw + C.bold + '  ⚡ ' + label + ' ' + C.rst +
    'Maior pressão: ' + C.wht + top.e.symbol + C.rst + ' (' + pressColor + pressureStr(top.m.pressure).trim() + C.rst + ')' +
    '  │  ' +
    'Maior delta 1min: ' + C.wht + topD.e.symbol + C.rst + ' (' +
      dColor + '$' + fmtVol(Math.abs(topD.m.winDelta)) + (topD.m.winDelta >= 0 ? ' compra' : ' venda') + C.rst + ')'
  )
}

function printAll() {
  process.stdout.write('\x1b[2J\x1b[H')   // limpar ecrã

  const now = new Date().toLocaleTimeString('pt-PT')
  log(C.cyn + C.bold + '\n  BINANCE FLOW MONITOR' + C.rst + C.dim + '  │  ' + now + '  │  Janela deslizante: ' + (CFG.WINDOW_MS/1000) + 's' + C.rst)

  // ── SPOT ────────────────────────────────────────────────────
  printHeader('SPOT', 'USDC')
  const spotEntries = STATE.spotSyms
    .map(s => STATE.spot.get(s))
    .filter(Boolean)

  spotEntries.forEach(entry => {
    const m = computeMetrics(entry)
    printRow(entry, m)
  })
  printSummary(spotEntries, 'spot')
  printTopMover(spotEntries, 'SPOT')

  // ── FUTURES ─────────────────────────────────────────────────
  printHeader('FUTURES PERP', 'USDT')
  const futEntries = STATE.futSyms
    .map(s => STATE.futures.get(s))
    .filter(Boolean)

  futEntries.forEach(entry => {
    const m = computeMetrics(entry)
    printRow(entry, m)
  })
  printSummary(futEntries, 'futures')
  printTopMover(futEntries, 'FUTURES')

  log('\n' + C.dim + '  Próxima actualização em ' + CFG.PRINT_SECS + 's  │  Ctrl+C para sair' + C.rst)
}

// ─── MÓDULO EXPORT (uso programático) ────────────────────────

/**
 * Acesso aos dados em bruto para integração noutros módulos.
 * Exemplo:
 *   const bf = require('./binance-flow')
 *   await bf.start()
 *   setInterval(() => {
 *     const snap = bf.snapshot()
 *     console.log(snap.spot[0])   // { symbol, buyQuote, sellQuote, buyRatio, pressure, signal, ... }
 *   }, 5000)
 */
function snapshot() {
  const mapEntries = (syms, map) =>
    syms
      .map(s => map.get(s))
      .filter(Boolean)
      .map(entry => {
        const m = computeMetrics(entry)
        return {
          symbol     : entry.symbol,
          market     : entry.market,
          rank       : entry.rank,
          price      : entry.price,
          change24h  : entry.change24h,
          quoteVol24h: entry.quoteVol24h,
          buyQuote   : entry.buyQuote,
          sellQuote  : entry.sellQuote,
          buyQty     : entry.buyQty,
          sellQty    : entry.sellQty,
          tradeCount : entry.tradeCount,
          buyRatio   : m.buyRatio,
          winBuyRatio: m.winBuyRatio,
          winDelta   : m.winDelta,
          pressure   : m.pressure,
          signal     : m.signal.trim(),
          lastTrade  : entry.lastTrade,
        }
      })

  return {
    spot   : mapEntries(STATE.spotSyms, STATE.spot),
    futures: mapEntries(STATE.futSyms,  STATE.futures),
    ts     : Date.now(),
  }
}

// ─── INÍCIO ──────────────────────────────────────────────────

async function start() {
  if (STATE.running) return
  STATE.running = true

  log(C.cyn + C.bold + '\n  ═══════════════════════════════════' + C.rst)
  log(C.cyn + C.bold + '  BINANCE FLOW MONITOR — iniciando...' + C.rst)
  log(C.cyn + C.bold + '  ═══════════════════════════════════\n' + C.rst)

  // 1. Carga inicial
  try {
    await fetchTopSpotUSDC()
    await fetchTopFuturesUSDT()
  } catch (err) {
    log(C.red + '  ✗ Erro na carga REST: ' + err.message + C.rst)
    process.exit(1)
  }

  // 2. Conectar WebSockets
  connectSpotWS()
  connectFuturesWS()

  // 3. Output periódico
  setTimeout(() => {
    printAll()
    setInterval(printAll, CFG.PRINT_SECS * 1000)
  }, 2000)

  // 4. Refresh periódico do ranking (mantém top 25 actualizado)
  setInterval(async () => {
    log(C.dim + '\n  [refresh] a actualizar ranking de volume...' + C.rst)
    try {
      await fetchTopSpotUSDC()
      await fetchTopFuturesUSDT()
      reconnectWS()
    } catch (err) {
      log(C.red + '  ✗ Erro no refresh: ' + err.message + C.rst)
    }
  }, CFG.REFRESH_SECS * 1000)
}

// ─── GRACEFUL SHUTDOWN ───────────────────────────────────────

process.on('SIGINT', () => {
  log('\n\n' + C.ylw + '  Encerrando...' + C.rst)
  ;['spot','futures'].forEach(k => {
    if (STATE.ws[k]) try { STATE.ws[k].close() } catch (_) {}
  })
  process.exit(0)
})

// ─── ENTRADA ─────────────────────────────────────────────────

// Execução directa: node binance-flow.js
if (require.main === module) {
  start().catch(err => {
    log(C.red + '  Erro fatal: ' + err.message + C.rst)
    process.exit(1)
  })
}

module.exports = { start, snapshot, STATE, CFG }
