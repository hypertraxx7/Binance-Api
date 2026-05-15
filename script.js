/**
 * binance-flow2.js  v2.0.0
 * ═══════════════════════════════════════════════════════════════
 * Recolha de dados em tempo real — Binance WebSocket
 *   • Top 25 Spot USDC         wss://stream.binance.com
 *   • Top 25 USDⓈ-M Perpetual  wss://fstream.binance.com
 *
 * Streams subscritos (aggTrade obrigatório; resto opcional por mercado)
 * ───────────────────────────────────────────────────────────────
 * Spot    : @aggTrade  @miniTicker  @bookTicker
 * Futures : @aggTrade  @markPrice@1s  @miniTicker  @bookTicker
 *
 * aggTrade payload:
 *   s  symbol | p price | q qty | m isBuyerMaker | T tradeTime
 *   m=false → BUY taker (comprador agressivo)
 *   m=true  → SELL taker (vendedor agressivo)
 *
 * markPriceUpdate payload (futures only):
 *   p markPrice | i indexPrice | r fundingRate | T nextFundingTime
 *
 * API PÚBLICA
 * ───────────────────────────────────────────────────────────────
 * BinanceFlow.start(opts)          → Promise<void>
 * BinanceFlow.stop()
 * BinanceFlow.on(event, fn)        → unsubscribe fn
 * BinanceFlow.off(event, fn)
 * BinanceFlow.getSnapshot()        → { spot[], futures[], ts }
 * BinanceFlow.getSummary(mkt)      → dados agregados
 * BinanceFlow.getEntry(sym, mkt)   → Entry | null
 *
 * EVENTOS
 * ───────────────────────────────────────────────────────────────
 * 'ready'    — WS conectados. payload: snapshot completo
 * 'tick'     — cada aggTrade. payload: Entry
 * 'extreme'  — sinal cruzou extreme_buy|extreme_sell. payload: Entry
 * 'mark'     — markPriceUpdate (futures). payload: Entry
 * 'update'   — snapshot periódico 1s. payload: { spot[], futures[] }
 * 'ranking'  — ranking REST recarregado. payload: { mkt, symbols }
 * 'status'   — estado WS. payload: { mkt, status }
 * ═══════════════════════════════════════════════════════════════
 */
;(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    const WS  = typeof WebSocket !== 'undefined' ? WebSocket : (() => { try { return require('ws') } catch (_) { throw new Error('npm install ws') } })()
    const ft  = typeof fetch !== 'undefined' ? fetch.bind(globalThis) : (() => { try { return require('node-fetch') } catch (_) { throw new Error('npm install node-fetch') } })()
    module.exports = factory(WS, ft)
  } else {
    root.BinanceFlow = factory(WebSocket, fetch.bind(window))
  }
}(typeof globalThis !== 'undefined' ? globalThis : this, function (WSImpl, fetchFn) {
  'use strict'

  // ─── ENDPOINTS ──────────────────────────────────────────────

  const EP = {
    spot: {
      rest: 'https://api.binance.com/api/v3/ticker/24hr',
      exch: 'https://api.binance.com/api/v3/exchangeInfo',
      ws:   'wss://stream.binance.com:9443/stream',
    },
    futures: {
      rest: 'https://fapi.binance.com/fapi/v1/ticker/24hr',
      exch: 'https://fapi.binance.com/fapi/v1/exchangeInfo',
      ws:   'wss://fstream.binance.com/stream',
    },
  }

  // ─── DEFAULTS ───────────────────────────────────────────────

  const DEFAULTS = {
    topN:           25,
    spotQuote:      'USDC',
    futuresQuote:   'USDT',
    windowMs:       60_000,    // janela deslizante 1 min
    window5Ms:      300_000,   // janela deslizante 5 min
    refreshMs:      30_000,    // re-rank REST
    reconnectMs:    3_000,
    maxPerConn:     350,       // streams por conexão WS (max 1024)
    updateHz:       1_000,     // evento 'update' a cada N ms
    excludeKw:      ['UP','DOWN','BULL','BEAR','3L','3S','LEVERAGE','HEDGE','_'],
    debug:          false,
  }

  // ─── ESTADO ─────────────────────────────────────────────────

  const S = {
    opts:      null,
    perps:     new Set(),          // perpétuos válidos (via exchangeInfo)
    symbols:   { spot: [], futures: [] },
    entries:   { spot: new Map(), futures: new Map() },
    conns:     { spot: [], futures: [] },
    listeners: new Map(),
    running:   false,
    timers:    [],
  }

  // ─── ENTRY ──────────────────────────────────────────────────

  function mkEntry (symbol, mkt, rank, t) {
    const vol      = +t.quoteVolume  || 0
    const takerBuy = +t.takerBuyQuoteAssetVolume || 0
    return {
      symbol, mkt, rank,
      quote: mkt === 'spot' ? S.opts.spotQuote : S.opts.futuresQuote,

      // Preço
      lastPrice:   +t.lastPrice,
      prevPrice:   +t.lastPrice,
      markPrice:   0,
      indexPrice:  0,
      basisPct:    0,
      openPrice:   +t.openPrice,
      highPrice:   +t.highPrice,
      lowPrice:    +t.lowPrice,
      change24h:   +t.priceChangePercent,

      // Funding (só futures)
      fundingRate:     0,
      fundingRatePct:  0,
      nextFundingTime: 0,
      msToFunding:     0,

      // Book
      bidPrice: 0, bidQty: 0,
      askPrice: 0, askQty: 0,
      spread: 0, spreadPct: 0,

      // 24h (seed REST — vai actualizar via miniTicker)
      vol24hBase:  +t.volume  || 0,
      vol24hQuote: vol,
      taker24hBuyVol:   takerBuy,
      taker24hSellVol:  vol - takerBuy,
      taker24hBuyRatio: vol > 0 ? takerBuy / vol * 100 : 50,

      // Sessão WS acumulada
      sessBuyVol:   0, sessSellVol:  0,
      sessBuyQty:   0, sessSellQty:  0,
      sessTradeCount: 0,
      sessBuyImpact: 0, sessSellImpact: 0,

      // Janela 1 min
      w1BuyVol: 0, w1SellVol: 0, w1Delta: 0,
      w1BuyImpact: 0, w1SellImpact: 0, w1Trades: 0,

      // Janela 5 min
      w5BuyVol: 0, w5SellVol: 0, w5Delta: 0, w5Trades: 0,

      // Derivados
      sessRatio:  50,
      w1Ratio:    50,
      w5Ratio:    50,
      pressure:   0,
      impactRatio: 50,
      signal:     'NEUTRO',
      signalKey:  'neutral',
      prevSignalKey: 'neutral',

      lastTradeTs: 0,
      _trades: [],
    }
  }

  // ─── REST ────────────────────────────────────────────────────

  async function restGet (url) {
    const r = await fetchFn(url)
    if (!r.ok) throw new Error(`HTTP ${r.status} — ${url}`)
    return r.json()
  }

  /** Carrega perpétuos válidos da exchangeInfo de futures */
  async function loadPerps () {
    try {
      const info = await restGet(EP.futures.exch)
      S.perps.clear()
      for (const s of (info.symbols || [])) {
        if (s.contractType !== 'PERPETUAL') continue
        if (s.status !== 'TRADING') continue
        if (S.opts.excludeKw.some(k => s.symbol.includes(k))) continue
        S.perps.add(s.symbol)
      }
      _log(`[exchangeInfo] ${S.perps.size} perpétuos válidos`)
    } catch (err) {
      _log('[exchangeInfo] erro:', err.message, '— usando fallback')
      // Fallback: confiar apenas no sufixo USDT
    }
  }

  async function loadRanking (mkt) {
    const isSpot = mkt === 'spot'
    const quote  = isSpot ? S.opts.spotQuote : S.opts.futuresQuote
    const tickers = await restGet(EP[mkt].rest)
    const opts    = S.opts

    const filtered = tickers
      .filter(t => {
        if (!t.symbol.endsWith(quote)) return false
        if (opts.excludeKw.some(k => t.symbol.includes(k))) return false
        if (!isSpot && S.perps.size > 0 && !S.perps.has(t.symbol)) return false
        return true
      })
      .sort((a, b) => +b.quoteVolume - +a.quoteVolume)
      .slice(0, opts.topN)

    const newSyms = filtered.map(t => t.symbol)
    S.symbols[mkt] = newSyms

    filtered.forEach((t, i) => {
      const existing = S.entries[mkt].get(t.symbol)
      if (existing) {
        // Actualizar só campos REST — não apagar acumuladores WS
        existing.rank         = i + 1
        existing.openPrice    = +t.openPrice
        existing.highPrice    = +t.highPrice
        existing.lowPrice     = +t.lowPrice
        existing.change24h    = +t.priceChangePercent
        existing.vol24hBase   = +t.volume
        existing.vol24hQuote  = +t.quoteVolume
        const tb = +t.takerBuyQuoteAssetVolume || 0
        existing.taker24hBuyVol   = tb
        existing.taker24hSellVol  = +t.quoteVolume - tb
        existing.taker24hBuyRatio = +t.quoteVolume > 0 ? tb / +t.quoteVolume * 100 : 50
      } else {
        S.entries[mkt].set(t.symbol, mkEntry(t.symbol, mkt, i + 1, t))
      }
    })

    _log(`[${mkt}] ranking: ${newSyms.length} símbolos`)
    emit('ranking', { mkt, symbols: newSyms })
  }

  // ─── PROCESSADORES WS ────────────────────────────────────────

  function onAggTrade (mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return

    const price  = +d.p
    const qty    = +d.q
    const vol    = price * qty
    const isBuy  = !d.m        // m=false → BUY taker
    const ts     = d.T

    const pDelta = e.lastTradeTs > 0 ? price - e.lastPrice : 0

    const prevKey    = e.signalKey
    e.prevPrice      = e.lastPrice
    e.lastPrice      = price
    e.lastTradeTs    = ts

    if (isBuy) {
      e.sessBuyVol += vol;  e.sessBuyQty += qty
      if (pDelta > 0) e.sessBuyImpact  += Math.abs(pDelta) * vol
    } else {
      e.sessSellVol += vol; e.sessSellQty += qty
      if (pDelta < 0) e.sessSellImpact += Math.abs(pDelta) * vol
    }
    e.sessTradeCount++

    e._trades.push({ ts, vol, isBuy, pDelta })
    const cut5 = ts - S.opts.window5Ms
    while (e._trades.length && e._trades[0].ts < cut5) e._trades.shift()

    _recompute(e, ts)

    // Guardar sinal anterior para detectar cruzamento
    e.prevSignalKey = prevKey

    emit('tick', snap(e))

    // Emitir 'extreme' apenas quando cruza para extremo
    if (
      (e.signalKey === 'extreme_buy' || e.signalKey === 'extreme_sell') &&
      e.signalKey !== prevKey
    ) {
      emit('extreme', snap(e))
    }
  }

  function onMarkPrice (mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return
    e.markPrice      = +d.p
    e.indexPrice     = +d.i
    e.fundingRate    = +d.r
    e.fundingRatePct = +d.r * 100
    e.nextFundingTime = +d.T
    e.msToFunding    = +d.T - Date.now()
    if (e.lastPrice > 0 && e.markPrice > 0) {
      e.basisPct = (e.lastPrice - e.markPrice) / e.markPrice * 100
    }
    emit('mark', snap(e))
  }

  function onMiniTicker (mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return
    e.prevPrice   = e.lastPrice
    e.lastPrice   = +d.c
    e.openPrice   = +d.o
    e.highPrice   = +d.h
    e.lowPrice    = +d.l
    e.vol24hBase  = +d.v
    e.vol24hQuote = +d.q
    if (+d.o > 0) e.change24h = (+d.c - +d.o) / +d.o * 100
  }

  function onBookTicker (mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return
    e.bidPrice = +d.b; e.bidQty = +d.B
    e.askPrice = +d.a; e.askQty = +d.A
    e.spread    = +d.a - +d.b
    e.spreadPct = +d.b > 0 ? (+d.a - +d.b) / +d.b * 100 : 0
  }

  // ─── DERIVADOS ───────────────────────────────────────────────

  function _recompute (e, nowTs) {
    const { windowMs, window5Ms } = S.opts
    const cut1 = nowTs - windowMs
    const cut5 = nowTs - window5Ms
    let b1=0, s1=0, bi1=0, si1=0, t1=0
    let b5=0, s5=0, t5=0
    for (const tr of e._trades) {
      if (tr.ts >= cut5) { tr.isBuy ? b5+=tr.vol : s5+=tr.vol; t5++ }
      if (tr.ts >= cut1) {
        if (tr.isBuy) { b1+=tr.vol; if (tr.pDelta>0) bi1+=Math.abs(tr.pDelta)*tr.vol }
        else           { s1+=tr.vol; if (tr.pDelta<0) si1+=Math.abs(tr.pDelta)*tr.vol }
        t1++
      }
    }
    e.w1BuyVol=b1; e.w1SellVol=s1; e.w1Delta=b1-s1
    e.w1BuyImpact=bi1; e.w1SellImpact=si1; e.w1Trades=t1
    e.w5BuyVol=b5; e.w5SellVol=s5; e.w5Delta=b5-s5; e.w5Trades=t5

    const tS=e.sessBuyVol+e.sessSellVol, t1T=b1+s1, t5T=b5+s5
    e.sessRatio = tS > 0 ? e.sessBuyVol/tS*100 : 50
    e.w1Ratio   = t1T > 0 ? b1/t1T*100 : 50
    e.w5Ratio   = t5T > 0 ? b5/t5T*100 : 50

    // Pressão: -100..+100 (janela recente tem mais peso)
    e.pressure = (e.w1Ratio-50)*2*0.55 + (e.w5Ratio-50)*2*0.30 + (e.sessRatio-50)*2*0.15

    const ti = e.sessBuyImpact + e.sessSellImpact
    e.impactRatio = ti > 0 ? e.sessBuyImpact/ti*100 : 50

    const p = e.pressure
    if      (p >=  72) { e.signalKey='extreme_buy';  e.signal='COMPRA EXTREMA'  }
    else if (p >=  38) { e.signalKey='strong_buy';   e.signal='COMPRA FORTE'    }
    else if (p >=  12) { e.signalKey='buy';          e.signal='COMPRADORES'     }
    else if (p >= -12) { e.signalKey='neutral';      e.signal='NEUTRO'          }
    else if (p >= -38) { e.signalKey='sell';         e.signal='VENDEDORES'      }
    else if (p >= -72) { e.signalKey='strong_sell';  e.signal='VENDA FORTE'     }
    else                { e.signalKey='extreme_sell'; e.signal='VENDA EXTREMA'  }
  }

  // ─── SNAPSHOT ────────────────────────────────────────────────

  function snap (e) {
    const { _trades, ...out } = e
    return { ...out, _ts: Date.now() }
  }

  function getSnapshot () {
    return {
      spot:    S.symbols.spot.map(s => S.entries.spot.get(s)).filter(Boolean).map(snap),
      futures: S.symbols.futures.map(s => S.entries.futures.get(s)).filter(Boolean).map(snap),
      ts:      Date.now(),
    }
  }

  function getSummary (mkt) {
    const entries = S.symbols[mkt].map(s => S.entries[mkt].get(s)).filter(Boolean)
    if (!entries.length) return null
    let tB=0, tS=0, tBI=0, tSI=0, w1B=0, w1S=0
    for (const e of entries) {
      tB+=e.sessBuyVol; tS+=e.sessSellVol
      tBI+=e.sessBuyImpact; tSI+=e.sessSellImpact
      w1B+=e.w1BuyVol; w1S+=e.w1SellVol
    }
    const tv=tB+tS, ti=tBI+tSI, tw=w1B+w1S
    const sessRatio  = tv > 0 ? tB/tv*100 : 50
    const impactRatio= ti > 0 ? tBI/ti*100 : 50
    const w1Ratio    = tw > 0 ? w1B/tw*100 : 50
    const byW1 = [...entries].sort((a,b)=>Math.abs(b.w1Delta)-Math.abs(a.w1Delta))
    const byP  = [...entries].sort((a,b)=>Math.abs(b.pressure)-Math.abs(a.pressure))
    const byF  = mkt==='futures' ? [...entries].sort((a,b)=>Math.abs(b.fundingRate)-Math.abs(a.fundingRate)) : []
    return {
      mkt, sessRatio, impactRatio, w1Ratio,
      totalBuyVol: tB, totalSellVol: tS, totalVol: tv,
      delta: tB-tS, w1Delta: w1B-w1S,
      dominantSide:  sessRatio>=50 ? 'buy' : 'sell',
      priceMover:    impactRatio>=50 ? 'buyers' : 'sellers',
      topW1Mover:    byW1[0] ? snap(byW1[0]) : null,
      topPressure:   byP[0] ? snap(byP[0]) : null,
      topFunding:    byF[0] ? snap(byF[0]) : null,
      count: entries.length, ts: Date.now(),
    }
  }

  // ─── WEBSOCKET ───────────────────────────────────────────────

  function buildStreams (syms, mkt) {
    const isSpot = mkt === 'spot'
    const list = []
    for (const s of syms) {
      const sl = s.toLowerCase()
      list.push(sl + '@aggTrade')
      list.push(sl + '@miniTicker')
      list.push(sl + '@bookTicker')
      if (!isSpot) list.push(sl + '@markPrice@1s')
    }
    return list
  }

  function openStreams (mkt) {
    // Fechar conexões antigas
    for (const c of S.conns[mkt]) {
      c.closing = true
      try { c.ws.close() } catch (_) {}
    }
    S.conns[mkt] = []

    const syms   = S.symbols[mkt]
    const allSt  = buildStreams(syms, mkt)
    const sz     = S.opts.maxPerConn
    const base   = EP[mkt].ws

    for (let i = 0; i < allSt.length; i += sz) {
      const group = allSt.slice(i, i + sz)
      const url   = base + '?streams=' + group.join('/')
      const conn  = { ws: null, closing: false, reconnects: 0 }
      S.conns[mkt].push(conn)
      _connectWS(conn, url, mkt, S.conns[mkt].length - 1)
    }
  }

  function _connectWS (conn, url, mkt, idx) {
    _log(`[WS ${mkt}#${idx}] conectando`)
    const ws = new WSImpl(url)
    conn.ws  = ws

    ws.onopen = () => {
      _log(`[WS ${mkt}#${idx}] aberto`)
      conn.reconnects = 0
      emit('status', { mkt, status: 'connected', idx })
      _checkReady()
    }

    ws.onmessage = ev => {
      let msg
      try { msg = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString()) }
      catch (_) { return }
      const d = msg.data || msg
      if (!d?.e) return
      switch (d.e) {
        case 'aggTrade':          onAggTrade(mkt, d);   break
        case 'markPriceUpdate':   onMarkPrice(mkt, d);  break
        case '24hrMiniTicker':    onMiniTicker(mkt, d); break
        case 'bookTicker':        onBookTicker(mkt, d); break
      }
    }

    ws.onerror = () => emit('status', { mkt, status: 'error', idx })

    ws.onclose = () => {
      if (conn.closing) return
      const delay = Math.min(S.opts.reconnectMs * (1 + conn.reconnects * 0.5), 15_000)
      conn.reconnects++
      emit('status', { mkt, status: 'reconnecting', idx, attempt: conn.reconnects })
      setTimeout(() => _connectWS(conn, url, mkt, idx), delay)
    }
  }

  function _checkReady () {
    const allOpen = ['spot','futures'].every(mkt =>
      S.conns[mkt].length > 0 && S.conns[mkt].every(c => c.ws?.readyState === 1)
    )
    if (allOpen) emit('ready', getSnapshot())
  }

  // ─── EVENTOS ─────────────────────────────────────────────────

  function on (event, fn) {
    if (!S.listeners.has(event)) S.listeners.set(event, new Set())
    S.listeners.get(event).add(fn)
    return () => off(event, fn)
  }

  function off (event, fn) { S.listeners.get(event)?.delete(fn) }

  function emit (event, data) {
    const ls = S.listeners.get(event)
    if (!ls?.size) return
    ls.forEach(fn => { try { fn(data) } catch (err) { console.error('[BinanceFlow] listener error:', err) } })
  }

  // ─── START / STOP ────────────────────────────────────────────

  async function start (opts = {}) {
    if (S.running) { console.warn('[BinanceFlow] já em execução — chama stop() primeiro'); return }
    S.opts    = { ...DEFAULTS, ...opts }
    S.running = true
    _log('iniciando...', S.opts)

    // 1. Perpétuos válidos (futures)
    await loadPerps()

    // 2. Ranking REST (spot + futures em paralelo)
    await Promise.all(['spot','futures'].map(loadRanking))

    // 3. Abrir streams WS
    openStreams('spot')
    openStreams('futures')

    // 4. Evento 'update' periódico
    S.timers.push(setInterval(() => emit('update', getSnapshot()), S.opts.updateHz))

    // 5. Refresh REST periódico
    S.timers.push(setInterval(async () => {
      _log('[refresh] a actualizar rankings...')
      try {
        await loadPerps()
        const ops = ['spot','futures'].map(async mkt => {
          const old = S.symbols[mkt].join()
          await loadRanking(mkt)
          if (S.symbols[mkt].join() !== old) openStreams(mkt)
        })
        await Promise.all(ops)
      } catch (err) { _log('[refresh] erro:', err.message) }
    }, S.opts.refreshMs))

    _log('iniciado ✓')
  }

  function stop () {
    S.running = false
    S.timers.forEach(t => clearInterval(t))
    S.timers = []
    ;['spot','futures'].forEach(mkt => {
      for (const c of S.conns[mkt]) { c.closing=true; try { c.ws.close() } catch(_){} }
      S.conns[mkt] = []
    })
    emit('status', { status: 'stopped' })
    _log('parado ✓')
  }

  async function restart (opts) {
    stop()
    S.running = false
    S.entries = { spot: new Map(), futures: new Map() }
    S.symbols = { spot: [], futures: [] }
    await new Promise(r => setTimeout(r, 500))
    await start(opts || S.opts)
  }

  // ─── UTILITÁRIOS EXPORTADOS ──────────────────────────────────

  function fmtVol (n) {
    if (n==null||isNaN(n)) return '—'
    if (n>=1e9) return (n/1e9).toFixed(2)+'B'
    if (n>=1e6) return (n/1e6).toFixed(2)+'M'
    if (n>=1e3) return (n/1e3).toFixed(1)+'K'
    return n.toFixed(2)
  }

  function fmtPrice (n) {
    if (!n) return '—'
    if (n>=10000) return n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})
    if (n>=100)   return n.toFixed(3)
    if (n>=1)     return n.toFixed(4)
    if (n>=0.01)  return n.toFixed(5)
    return n.toFixed(6)
  }

  function fmtPct (n, decimals=2) {
    if (n==null||isNaN(n)) return '—'
    return (n>=0?'+':'')+n.toFixed(decimals)+'%'
  }

  function fmtFunding (rate) {
    if (rate==null||isNaN(rate)) return '—'
    return (rate*100).toFixed(4)+'%'
  }

  function fmtCountdown (ms) {
    if (!ms||ms<=0) return '—'
    const h=Math.floor(ms/3600000)
    const m=Math.floor((ms%3600000)/60000)
    const s=Math.floor((ms%60000)/1000)
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
  }

  function _log (...a) { if (S.opts?.debug) console.log('[BinanceFlow]', ...a) }

  function getConnStatus () {
    return ['spot','futures'].reduce((o,mkt) => {
      o[mkt] = S.conns[mkt].map((c,i) => ({
        idx:i, reconnects:c.reconnects,
        state:['CONNECTING','OPEN','CLOSING','CLOSED'][c.ws?.readyState]||'?'
      }))
      return o
    },{})
  }

  // ─── API ─────────────────────────────────────────────────────

  return {
    start, stop, restart,
    on, off,
    getSnapshot, getSummary,
    getEntry: (sym, mkt='spot') => { const e=S.entries[mkt].get(sym); return e?snap(e):null },
    getConnStatus,
    fmtVol, fmtPrice, fmtPct, fmtFunding, fmtCountdown,
    get symbols ()  { return { spot:[...S.symbols.spot], futures:[...S.symbols.futures] } },
    get running ()  { return S.running },
    get version ()  { return '2.0.0' },
  }
}))