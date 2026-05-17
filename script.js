/**
 * binance-terminal.js  v3.0.0
 * ═══════════════════════════════════════════════════════════════
 * Módulo de dados em tempo real — Binance WebSocket
 *
 * Mercados suportados:
 *   • Spot USDC         — wss://stream.binance.com:9443
 *   • USDⓈ-M Futures   — wss://fstream.binance.com
 *     (apenas contractType=PERPETUAL via exchangeInfo)
 *
 * Streams por símbolo:
 * ───────────────────────────────────────────────────────────────
 *   Spot    : @aggTrade  @miniTicker
 *   Futures : @aggTrade  @miniTicker  @markPrice@1s
 *
 * Order Book (subscrito individualmente por símbolo seleccionado):
 *   Spot    : @depth{N}@100ms   (N = 5 | 10 | 20)
 *   Futures : @depth{N}@100ms
 *   Payload : { bids: [["price","qty"],...], asks: [...] }
 *             (partial book depth snapshot — top N levels)
 *
 * aggTrade payload relevante:
 *   s  symbol | p price | q qty | m isBuyerMaker | T timestamp
 *   m=false → BUY taker (comprador agressivo)
 *   m=true  → SELL taker (vendedor agressivo)
 *
 * markPriceUpdate payload (futures only):
 *   p markPrice | i indexPrice | r fundingRate | T nextFundingTime
 *
 * ══════════════════════════════════════════════════════════════
 * EVENTOS EMITIDOS
 * ══════════════════════════════════════════════════════════════
 *   'ready'    — WS conectados. payload: snapshot completo
 *   'tick'     — aggTrade processado. payload: Entry
 *   'mark'     — markPriceUpdate. payload: Entry (futures)
 *   'book'     — order book actualizado. payload: { sym, mkt, bids, asks, spread }
 *   'update'   — snapshot periódico 1s. payload: { spot[], futures[] }
 *   'ranking'  — ranking REST recarregado. payload: { mkt, symbols }
 *   'status'   — estado WS. payload: { id, status }
 *
 * ══════════════════════════════════════════════════════════════
 * USO (Browser)
 * ══════════════════════════════════════════════════════════════
 *   <script src="binance-terminal.js"></script>
 *   <script>
 *     await BinanceTerm.start({ topN: 25 })
 *     BinanceTerm.on('update', snap => renderUI(snap))
 *     BinanceTerm.on('book',   book => renderOrderBook(book))
 *
 *     // Subscrever order book de um símbolo
 *     BinanceTerm.subscribeBook('BTCUSDC', 'spot', 10)
 *     BinanceTerm.subscribeBook('BTCUSDT', 'futures', 20)
 *   </script>
 *
 * ══════════════════════════════════════════════════════════════
 * USO (Node.js)
 * ══════════════════════════════════════════════════════════════
 *   npm install ws node-fetch
 *   const T = require('./binance-terminal')
 *   await T.start({ topN: 25, debug: true })
 *   T.on('update', s => console.log(T.getSummary('spot')))
 * ═══════════════════════════════════════════════════════════════
 */

;(function (root, factory) {
  if (typeof module !== 'undefined' && module.exports) {
    const WS = typeof WebSocket !== 'undefined' ? WebSocket
      : (() => { try { return require('ws') } catch (_) { throw new Error('npm install ws') } })()
    const ft = typeof fetch !== 'undefined' ? fetch.bind(globalThis)
      : (() => { try { return require('node-fetch') } catch (_) { throw new Error('npm install node-fetch') } })()
    module.exports = factory(WS, ft)
  } else {
    root.BinanceTerm = factory(WebSocket, fetch.bind(window))
  }
}(typeof globalThis !== 'undefined' ? globalThis : this, function (WSImpl, fetchFn) {
  'use strict'

  // ─── ENDPOINTS ──────────────────────────────────────────────

  const EP = {
    spot: {
      rest:  'https://api.binance.com/api/v3/ticker/24hr',
      ws:    'wss://stream.binance.com:9443/stream',
    },
    futures: {
      rest:  'https://fapi.binance.com/fapi/v1/ticker/24hr',
      exch:  'https://fapi.binance.com/fapi/v1/exchangeInfo',
      ws:    'wss://fstream.binance.com/stream',
    },
  }

  // ─── DEFAULTS ───────────────────────────────────────────────

  const DEFAULTS = {
    topN:          25,
    spotQuote:     'USDC',
    futuresQuote:  'USDT',
    windowMs:      60_000,     // janela deslizante 1 min
    window5Ms:     300_000,    // janela deslizante 5 min
    refreshMs:     30_000,     // re-rank REST
    reconnectMs:   3_500,
    updateHz:      1_000,      // evento 'update' periódico (ms)
    maxPerConn:    350,        // streams por conexão WS
    excludeKw:     ['UP','DOWN','BULL','BEAR','3L','3S','LEVERAGE','HEDGE'],
    debug:         false,
  }

  // ─── ESTADO ─────────────────────────────────────────────────

  const S = {
    opts:      null,
    perps:     new Set(),
    symbols:   { spot: [], futures: [] },
    entries:   { spot: new Map(), futures: new Map() },
    conns:     { spot: [], futures: [] },
    bookConn:  null,              // conexão WS do order book activo
    bookSub:   null,              // { sym, mkt, depth }
    listeners: new Map(),
    running:   false,
    timers:    [],
  }

  // ─── ESTRUTURA ENTRY ─────────────────────────────────────────
  /**
   * @typedef {Object} Entry
   * @property {string}  sym              símbolo (ex: BTCUSDC)
   * @property {string}  mkt              'spot' | 'futures'
   * @property {number}  rank             posição no ranking de volume 24h
   * @property {number}  price            último preço
   * @property {number}  prevPrice        preço anterior
   * @property {number}  markPrice        mark price (futures only)
   * @property {number}  indexPrice       index price (futures only)
   * @property {number}  fundingRate      funding rate decimal (futures)
   * @property {number}  msToFunding      ms até próximo funding
   * @property {number}  change24h        variação % 24h
   * @property {number}  vol24h           volume 24h (USDC/USDT)
   *
   * — Sessão —
   * @property {number}  sessBuy          volume comprador acumulado (USDC/USDT)
   * @property {number}  sessSell         volume vendedor acumulado
   * @property {number}  sessCount        nº de trades
   * @property {number}  sessBuyImpact    impacto comprador no preço (vol-ponderado)
   * @property {number}  sessSellImpact   impacto vendedor no preço
   *
   * — Janela 1 min —
   * @property {number}  w1Buy            volume comprador janela 1min
   * @property {number}  w1Sell           volume vendedor janela 1min
   * @property {number}  w1Delta          buy - sell (positivo = compradores)
   *
   * — Janela 5 min —
   * @property {number}  w5Buy
   * @property {number}  w5Sell
   *
   * — Derivados —
   * @property {number}  sessRatio        % buy na sessão (0-100)
   * @property {number}  w1Ratio          % buy janela 1min
   * @property {number}  w5Ratio          % buy janela 5min
   * @property {number}  pressure         −100 (venda extrema) … +100 (compra extrema)
   * @property {number}  impactRatio      % impacto dos compradores no preço (0-100)
   * @property {string}  signal           'extreme_buy'|'strong_buy'|'buy'|'neutral'|'sell'|'strong_sell'|'extreme_sell'
   * @property {string}  sigTxt           texto legível do sinal
   */
  function mkEntry(sym, mkt, rank, t) {
    const vol = +t.quoteVolume || 0
    return {
      sym, mkt, rank,
      price: +t.lastPrice, prevPrice: +t.lastPrice,
      markPrice: 0, indexPrice: 0, fundingRate: 0, msToFunding: 0,
      openPrice: +t.openPrice, highPrice: +t.highPrice, lowPrice: +t.lowPrice,
      change24h: +t.priceChangePercent, vol24h: vol,
      sessBuy: 0, sessSell: 0, sessCount: 0,
      sessBuyImpact: 0, sessSellImpact: 0,
      w1Buy: 0, w1Sell: 0, w1Delta: 0, w1BI: 0, w1SI: 0,
      w5Buy: 0, w5Sell: 0,
      sessRatio: 50, w1Ratio: 50, w5Ratio: 50,
      pressure: 0, impactRatio: 50,
      signal: 'neutral', sigTxt: 'NEUTRO',
      lastTs: 0, _trades: [],
    }
  }

  // ─── REST ────────────────────────────────────────────────────

  async function get(url) {
    const r = await fetchFn(url)
    if (!r.ok) throw new Error(`HTTP ${r.status} — ${url}`)
    return r.json()
  }

  async function loadPerps() {
    try {
      const d = await get(EP.futures.exch)
      S.perps.clear()
      for (const s of d.symbols || []) {
        if (s.contractType !== 'PERPETUAL') continue
        if (s.status !== 'TRADING') continue
        if (S.opts.excludeKw.some(k => s.symbol.includes(k))) continue
        S.perps.add(s.symbol)
      }
      _log(`[perps] ${S.perps.size} perpétuos USDT válidos`)
    } catch (err) {
      _log('[perps] erro exchangeInfo:', err.message)
    }
  }

  async function loadRanking(mkt) {
    const isSpot = mkt === 'spot'
    const quote  = isSpot ? S.opts.spotQuote : S.opts.futuresQuote
    const tickers = await get(EP[mkt].rest)
    const filtered = tickers
      .filter(t => {
        if (!t.symbol.endsWith(quote)) return false
        if (S.opts.excludeKw.some(k => t.symbol.includes(k))) return false
        if (!isSpot && S.perps.size > 0 && !S.perps.has(t.symbol)) return false
        return true
      })
      .sort((a, b) => +b.quoteVolume - +a.quoteVolume)
      .slice(0, S.opts.topN)

    const newSyms = filtered.map(t => t.symbol)
    S.symbols[mkt] = newSyms
    filtered.forEach((t, i) => {
      const ex = S.entries[mkt].get(t.symbol)
      if (ex) {
        ex.rank = i + 1
        ex.change24h = +t.priceChangePercent
        ex.vol24h = +t.quoteVolume
      } else {
        S.entries[mkt].set(t.symbol, mkEntry(t.symbol, mkt, i + 1, t))
      }
    })
    _log(`[${mkt}] ranking: ${newSyms.length} símbolos`)
    emit('ranking', { mkt, symbols: newSyms })
  }

  // ─── PROCESSADORES ──────────────────────────────────────────

  /**
   * aggTrade — a base de tudo
   * m=false → comprador foi o taker agressivo → BUY
   * m=true  → vendedor foi o taker agressivo  → SELL
   */
  function onAggTrade(mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return

    const price = +d.p
    const qty   = +d.q
    const vol   = price * qty
    const buy   = !d.m
    const ts    = d.T
    const pd    = e.lastTs > 0 ? price - e.price : 0

    e.prevPrice  = e.price
    e.price      = price
    e.lastTs     = ts

    if (buy) {
      e.sessBuy += vol
      if (pd > 0) e.sessBuyImpact += Math.abs(pd) * vol
    } else {
      e.sessSell += vol
      if (pd < 0) e.sessSellImpact += Math.abs(pd) * vol
    }
    e.sessCount++

    e._trades.push({ ts, vol, buy, pd })
    const cut5 = ts - S.opts.window5Ms
    while (e._trades.length && e._trades[0].ts < cut5) e._trades.shift()

    _recompute(e, ts)
    emit('tick', _snap(e))
  }

  /** markPriceUpdate — apenas futures */
  function onMarkPrice(d) {
    const e = S.entries.futures.get(d.s)
    if (!e) return
    e.markPrice    = +d.p
    e.indexPrice   = +d.i
    e.fundingRate  = +d.r
    e.msToFunding  = +d.T - Date.now()
    emit('mark', _snap(e))
  }

  /** miniTicker — actualiza preço/volume 24h em tempo real */
  function onMiniTicker(mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return
    e.prevPrice = e.price
    e.price     = +d.c
    e.openPrice = +d.o
    e.highPrice = +d.h
    e.lowPrice  = +d.l
    e.vol24h    = +d.q
    if (+d.o > 0) e.change24h = (+d.c - +d.o) / +d.o * 100
  }

  // ─── DERIVADOS ───────────────────────────────────────────────

  function _recompute(e, nowTs) {
    const { windowMs, window5Ms } = S.opts
    const cut1 = nowTs - windowMs
    const cut5 = nowTs - window5Ms
    let b1=0, s1=0, bi1=0, si1=0, b5=0, s5=0
    for (const tr of e._trades) {
      if (tr.ts >= cut5) { tr.buy ? b5+=tr.vol : s5+=tr.vol }
      if (tr.ts >= cut1) {
        if (tr.buy) { b1+=tr.vol; if(tr.pd>0) bi1+=Math.abs(tr.pd)*tr.vol }
        else         { s1+=tr.vol; if(tr.pd<0) si1+=Math.abs(tr.pd)*tr.vol }
      }
    }
    e.w1Buy=b1; e.w1Sell=s1; e.w1Delta=b1-s1; e.w1BI=bi1; e.w1SI=si1
    e.w5Buy=b5; e.w5Sell=s5

    const tS=e.sessBuy+e.sessSell, t1=b1+s1, t5=b5+s5
    e.sessRatio = tS>0 ? e.sessBuy/tS*100 : 50
    e.w1Ratio   = t1>0 ? b1/t1*100 : 50
    e.w5Ratio   = t5>0 ? b5/t5*100 : 50

    // Pressão combinada −100…+100
    e.pressure = (e.w1Ratio-50)*2*.55 + (e.w5Ratio-50)*2*.30 + (e.sessRatio-50)*2*.15

    const ti = e.sessBuyImpact + e.sessSellImpact
    e.impactRatio = ti > 0 ? e.sessBuyImpact/ti*100 : 50

    const p = e.pressure
    if      (p >=  72) { e.signal='extreme_buy';  e.sigTxt='COMPRA MAX'   }
    else if (p >=  38) { e.signal='strong_buy';   e.sigTxt='COMPRA FORTE' }
    else if (p >=  12) { e.signal='buy';          e.sigTxt='COMPRADORES'  }
    else if (p >= -12) { e.signal='neutral';      e.sigTxt='NEUTRO'       }
    else if (p >= -38) { e.signal='sell';         e.sigTxt='VENDEDORES'   }
    else if (p >= -72) { e.signal='strong_sell';  e.sigTxt='VENDA FORTE'  }
    else                { e.signal='extreme_sell'; e.sigTxt='VENDA MAX'    }
  }

  // ─── SNAPSHOT ────────────────────────────────────────────────

  function _snap(e) {
    const { _trades, ...out } = e
    return { ...out, _ts: Date.now() }
  }

  function getAll() {
    return {
      spot:    S.symbols.spot.map(s => S.entries.spot.get(s)).filter(Boolean).map(_snap),
      futures: S.symbols.futures.map(s => S.entries.futures.get(s)).filter(Boolean).map(_snap),
      ts:      Date.now(),
    }
  }

  // ─── WEBSOCKET PRINCIPAL ────────────────────────────────────

  function _buildUrl(mkt) {
    const syms   = S.symbols[mkt]
    const base   = EP[mkt].ws
    const isSpot = mkt === 'spot'
    const list   = []
    for (const s of syms) {
      const sl = s.toLowerCase()
      list.push(sl + '@aggTrade')
      list.push(sl + '@miniTicker')
      if (!isSpot) list.push(sl + '@markPrice@1s')
    }
    const sz = S.opts.maxPerConn
    const groups = []
    for (let i = 0; i < list.length; i += sz) groups.push(list.slice(i, i+sz))
    return groups.map(g => base + '?streams=' + g.join('/'))
  }

  function openMain(mkt) {
    // Fechar conexões antigas
    for (const c of S.conns[mkt]) { c.closing=true; try { c.ws.close() } catch(_){} }
    S.conns[mkt] = []

    const urls = _buildUrl(mkt)
    urls.forEach((url, idx) => {
      const conn = { ws: null, closing: false, reconnects: 0 }
      S.conns[mkt].push(conn)
      _connectMain(conn, url, mkt, idx)
    })
  }

  function _connectMain(conn, url, mkt, idx) {
    _log(`[WS ${mkt}#${idx}] conectando`)
    const ws = new WSImpl(url)
    conn.ws = ws

    ws.onopen = () => {
      _log(`[WS ${mkt}#${idx}] aberto`)
      conn.reconnects = 0
      emit('status', { id: `${mkt}-${idx}`, status: 'connected' })
    }

    ws.onmessage = ev => {
      let msg
      try { msg = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString()) }
      catch (_) { return }
      const d = msg.data || msg
      if (!d?.e) return
      switch (d.e) {
        case 'aggTrade':         onAggTrade(mkt, d);   break
        case 'markPriceUpdate':  onMarkPrice(d);        break
        case '24hrMiniTicker':   onMiniTicker(mkt, d); break
      }
    }

    ws.onerror = () => emit('status', { id: `${mkt}-${idx}`, status: 'error' })

    ws.onclose = () => {
      if (conn.closing) return
      const delay = Math.min(S.opts.reconnectMs * (1 + conn.reconnects * 0.4), 20_000)
      conn.reconnects++
      _log(`[WS ${mkt}#${idx}] fechado — reconexão #${conn.reconnects} em ${Math.round(delay)}ms`)
      emit('status', { id: `${mkt}-${idx}`, status: 'reconnecting', attempt: conn.reconnects })
      setTimeout(() => _connectMain(conn, url, mkt, idx), delay)
    }
  }

  // ─── ORDER BOOK WEBSOCKET ───────────────────────────────────

  /**
   * Subscreve o order book de um símbolo.
   * Apenas uma subscrição activa de cada vez.
   *
   * @param {string} sym    símbolo (ex: BTCUSDC, BTCUSDT)
   * @param {string} mkt    'spot' | 'futures'
   * @param {number} depth  5 | 10 | 20
   *
   * Payload emitido no evento 'book':
   * {
   *   sym, mkt, depth,
   *   bids: [{ price, qty, total }],  // ordenado: melhor bid primeiro
   *   asks: [{ price, qty, total }],  // ordenado: melhor ask primeiro
   *   bestBid, bestAsk, spread, spreadPct,
   *   maxQty,   // para cálculo de barras de profundidade
   *   ts
   * }
   */
  function subscribeBook(sym, mkt, depth = 5) {
    // Fechar subscrição anterior
    if (S.bookConn) {
      S.bookConn.closing = true
      try { S.bookConn.ws.close() } catch (_) {}
      S.bookConn = null
    }

    S.bookSub = { sym, mkt, depth }
    const base   = EP[mkt].ws
    const stream = sym.toLowerCase() + '@depth' + depth + '@100ms'
    const url    = base + '?streams=' + stream
    const conn   = { ws: null, closing: false, reconnects: 0 }
    S.bookConn   = conn
    _connectBook(conn, url, sym, mkt, depth)
  }

  function unsubscribeBook() {
    if (S.bookConn) {
      S.bookConn.closing = true
      try { S.bookConn.ws.close() } catch (_) {}
      S.bookConn = null
    }
    S.bookSub = null
    emit('status', { id: 'book', status: 'closed' })
  }

  function _connectBook(conn, url, sym, mkt, depth) {
    _log(`[WS book] ${sym} depth${depth}`)
    const ws = new WSImpl(url)
    conn.ws  = ws

    ws.onopen = () => {
      conn.reconnects = 0
      emit('status', { id: 'book', status: 'connected', sym, mkt, depth })
    }

    ws.onmessage = ev => {
      let msg
      try { msg = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString()) }
      catch (_) { return }
      const d = msg.data || msg
      // Partial book depth: has bids + asks arrays
      const rawBids = d.bids || d.b
      const rawAsks = d.asks || d.a
      if (!rawBids || !rawAsks) return

      // Build enriched arrays
      const maxQty = Math.max(
        ...rawBids.map(r => +r[1]),
        ...rawAsks.map(r => +r[1]),
        1
      )

      let askCum = 0
      const asks = [...rawAsks]
        .sort((a, b) => +a[0] - +b[0])           // ascending price
        .map(r => {
          const price = +r[0], qty = +r[1]
          askCum += price * qty
          return { price, qty, total: askCum, depthPct: qty / maxQty * 100 }
        })

      let bidCum = 0
      const bids = [...rawBids]
        .sort((a, b) => +b[0] - +a[0])           // descending price (best first)
        .map(r => {
          const price = +r[0], qty = +r[1]
          bidCum += price * qty
          return { price, qty, total: bidCum, depthPct: qty / maxQty * 100 }
        })

      const bestBid   = bids[0]?.price || 0
      const bestAsk   = asks[0]?.price || 0
      const spread    = bestAsk > 0 ? bestAsk - bestBid : 0
      const spreadPct = bestBid > 0 ? spread / bestBid * 100 : 0

      emit('book', { sym, mkt, depth, bids, asks, bestBid, bestAsk, spread, spreadPct, maxQty, ts: Date.now() })
    }

    ws.onerror = () => emit('status', { id: 'book', status: 'error' })

    ws.onclose = () => {
      if (conn.closing) return
      // Só reconecta se o bookSub ainda for este símbolo
      if (!S.bookSub || S.bookSub.sym !== sym || S.bookSub.mkt !== mkt) return
      const delay = Math.min(S.opts.reconnectMs * (1 + conn.reconnects * 0.4), 15_000)
      conn.reconnects++
      setTimeout(() => _connectBook(conn, url, sym, mkt, depth), delay)
    }
  }

  // ─── SUMÁRIO ────────────────────────────────────────────────

  function getSummary(mkt) {
    const entries = S.symbols[mkt].map(s => S.entries[mkt].get(s)).filter(Boolean)
    if (!entries.length) return null
    let tB=0, tS=0, tBI=0, tSI=0, w1B=0, w1S=0
    for (const e of entries) {
      tB+=e.sessBuy; tS+=e.sessSell
      tBI+=e.sessBuyImpact; tSI+=e.sessSellImpact
      w1B+=e.w1Buy; w1S+=e.w1Sell
    }
    const tv=tB+tS, ti=tBI+tSI, tw=w1B+w1S
    const sessRatio  = tv>0 ? tB/tv*100 : 50
    const impactRatio= ti>0 ? tBI/ti*100 : 50
    const w1Ratio    = tw>0 ? w1B/tw*100 : 50
    const byW1   = [...entries].sort((a,b)=>Math.abs(b.w1Delta)-Math.abs(a.w1Delta))
    const byP    = [...entries].sort((a,b)=>Math.abs(b.pressure)-Math.abs(a.pressure))
    return {
      mkt, totalBuyVol:tB, totalSellVol:tS, totalVol:tv,
      delta:tB-tS, w1Delta:w1B-w1S,
      sessRatio, impactRatio, w1Ratio,
      dominantSide:  sessRatio>=50 ? 'buy' : 'sell',
      priceMover:    impactRatio>=50 ? 'buyers' : 'sellers',
      topW1Mover:    byW1[0] ? _snap(byW1[0]) : null,
      topPressure:   byP[0]  ? _snap(byP[0])  : null,
      count:         entries.length,
      ts:            Date.now(),
    }
  }

  // ─── EVENTOS ─────────────────────────────────────────────────

  function on(event, fn) {
    if (!S.listeners.has(event)) S.listeners.set(event, new Set())
    S.listeners.get(event).add(fn)
    return () => off(event, fn)
  }

  function off(event, fn) { S.listeners.get(event)?.delete(fn) }

  function emit(event, data) {
    const ls = S.listeners.get(event)
    if (!ls?.size) return
    ls.forEach(fn => { try { fn(data) } catch (err) { console.error('[BinanceTerm]', err) } })
  }

  // ─── START / STOP ─────────────────────────────────────────────

  /**
   * @param {Object} [opts]
   * @param {number}   [opts.topN=25]
   * @param {string}   [opts.spotQuote='USDC']      quote para spot
   * @param {string}   [opts.futuresQuote='USDT']   quote para futures (USDⓈ-M)
   * @param {number}   [opts.windowMs=60000]         janela 1 min
   * @param {number}   [opts.window5Ms=300000]        janela 5 min
   * @param {number}   [opts.refreshMs=30000]         refresh REST
   * @param {number}   [opts.reconnectMs=3500]
   * @param {boolean}  [opts.debug=false]
   */
  async function start(opts = {}) {
    if (S.running) { console.warn('[BinanceTerm] já em execução'); return }
    S.opts    = { ...DEFAULTS, ...opts }
    S.running = true
    _log('iniciando...', S.opts)

    await loadPerps()
    await Promise.all(['spot','futures'].map(loadRanking))

    openMain('spot')
    openMain('futures')

    S.timers.push(setInterval(() => emit('update', getAll()), S.opts.updateHz))

    S.timers.push(setInterval(async () => {
      try {
        await loadPerps()
        const ops = ['spot','futures'].map(async mkt => {
          const old = S.symbols[mkt].join()
          await loadRanking(mkt)
          if (S.symbols[mkt].join() !== old) openMain(mkt)
        })
        await Promise.all(ops)
      } catch (err) { _log('[refresh] erro:', err.message) }
    }, S.opts.refreshMs))

    emit('status', { id: 'init', status: 'started' })
    _log('iniciado ✓')
  }

  function stop() {
    S.running = false
    S.timers.forEach(t => clearInterval(t)); S.timers = []
    ;['spot','futures'].forEach(mkt => {
      for (const c of S.conns[mkt]) { c.closing=true; try { c.ws.close() } catch(_){} }
      S.conns[mkt] = []
    })
    unsubscribeBook()
    emit('status', { id: 'init', status: 'stopped' })
    _log('parado ✓')
  }

  async function restart(opts) {
    stop(); S.running=false
    S.entries = { spot: new Map(), futures: new Map() }
    S.symbols = { spot: [], futures: [] }
    await new Promise(r => setTimeout(r, 600))
    await start(opts || S.opts)
  }

  // ─── UTILIDADES ──────────────────────────────────────────────

  function fmtVol(n) {
    if (n==null||isNaN(n)) return '—'
    if (n>=1e9) return (n/1e9).toFixed(2)+'B'
    if (n>=1e6) return (n/1e6).toFixed(2)+'M'
    if (n>=1e3) return (n/1e3).toFixed(1)+'K'
    return n.toFixed(2)
  }

  function fmtPrice(n) {
    if (!n) return '—'
    if (n>=10000) return n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})
    if (n>=100)   return n.toFixed(3)
    if (n>=1)     return n.toFixed(4)
    if (n>=.01)   return n.toFixed(5)
    return n.toFixed(6)
  }

  function fmtPct(n, d=2) {
    if (n==null||isNaN(n)) return '—'
    return (n>=0?'+':'')+n.toFixed(d)+'%'
  }

  function fmtFunding(r) {
    if (r==null||isNaN(r)) return '—'
    return (r*100).toFixed(4)+'%'
  }

  function fmtCountdown(ms) {
    if (!ms||ms<=0) return '—'
    const h=Math.floor(ms/3600000), m=Math.floor((ms%3600000)/60000), s=Math.floor((ms%60000)/1000)
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
  }

  function _log(...a) { if (S.opts?.debug) console.log('[BinanceTerm]', ...a) }

  function getConnStatus() {
    const out = {}
    ;['spot','futures'].forEach(mkt => {
      out[mkt] = S.conns[mkt].map((c,i) => ({
        idx:i, reconnects:c.reconnects,
        state:['CONNECTING','OPEN','CLOSING','CLOSED'][c.ws?.readyState]||'?'
      }))
    })
    out.book = S.bookConn ? {
      reconnects: S.bookConn.reconnects,
      state: ['CONNECTING','OPEN','CLOSING','CLOSED'][S.bookConn.ws?.readyState]||'?',
      sub:   S.bookSub,
    } : null
    return out
  }

  // ─── API ─────────────────────────────────────────────────────

  return {
    // Lifecycle
    start, stop, restart,

    // Eventos
    on, off,

    // Dados
    getAll, getSummary,
    getEntry: (sym, mkt='spot') => { const e=S.entries[mkt].get(sym); return e?_snap(e):null },
    getConnStatus,

    // Order book
    subscribeBook,
    unsubscribeBook,
    get bookSub() { return S.bookSub ? { ...S.bookSub } : null },

    // Utilitários
    fmtVol, fmtPrice, fmtPct, fmtFunding, fmtCountdown,

    // Info
    get symbols()  { return { spot:[...S.symbols.spot], futures:[...S.symbols.futures] } },
    get running()  { return S.running },
    get version()  { return '3.0.0' },
  }
}))
