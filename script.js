/**
 * binance-v2.js  v4.0.0
 * ═══════════════════════════════════════════════════════════════
 * Módulo de dados em tempo real — Binance WebSocket
 *
 * ┌─────────────────────────────────────────────────────────────┐
 * │  MERCADO         ENDPOINT WebSocket                        │
 * ├─────────────────────────────────────────────────────────────┤
 * │  Spot USDC       wss://stream.binance.com:9443/stream      │
 * │  Fut (public)    wss://fstream.binance.com/public/stream   │
 * │  Fut (markPrice) wss://fstream.binance.com/market/stream   │
 * │  Fut (WS API)    wss://ws-fapi.binance.com/ws-fapi/v1      │
 * └─────────────────────────────────────────────────────────────┘
 *
 * Streams subscritos:
 * ───────────────────────────────────────────────────────────────
 * Spot    : <sym>@aggTrade  <sym>@miniTicker
 * Fut pub : <sym>@aggTrade  <sym>@miniTicker
 * Fut mkt : <sym>@markPrice@1s
 *           (requer rota /market — não funciona em /stream genérico)
 *
 * USDⓈ-M WebSocket API (wss://ws-fapi.binance.com/ws-fapi/v1):
 *   Protocolo request/response (como REST mas sobre WS).
 *   Usado para: depth snapshots sob pedido.
 *   Formato envio  : { "id": "uid", "method": "depth", "params": { "symbol", "limit" } }
 *   Formato resposta: { "id": "uid", "status": 200, "result": { "bids", "asks", "lastUpdateId" } }
 *
 * SIGNAL LOGIC (STICKY):
 *   O sinal apenas muda quando a pressão cruza os limiares extremos:
 *     pressure >= +72 → 'extreme_buy'  (🔥 COMPRA MAX)
 *     pressure <= -72 → 'extreme_sell' (💀 VENDA MAX)
 *   Entre extremos: o sinal fica no último estado extreme.
 *
 * EVENTOS:
 *   'ready'    — todos os WS conectados. payload: snapshot
 *   'tick'     — aggTrade processado. payload: Entry
 *   'extreme'  — sinal mudou para extremo. payload: Entry
 *   'mark'     — markPriceUpdate. payload: Entry
 *   'book'     — order book actualizado. payload: BookData
 *   'update'   — snapshot periódico 1s. payload: { spot[], futures[] }
 *   'ranking'  — ranking REST recarregado. payload: { mkt, symbols }
 *   'status'   — estado de conexão WS. payload: { id, status }
 *
 * USO (Browser):
 *   <script src="binance-v2.js"></script>
 *   <script>
 *     await BinanceV2.start({ topN: 25 })
 *     BinanceV2.on('update',  snap => render(snap))
 *     BinanceV2.on('extreme', entry => alert(entry.sym + ': ' + entry.sigTxt))
 *     BinanceV2.on('book',    book => renderBook(book))
 *     BinanceV2.subscribeBook('BTCUSDT', 'futures', 10)
 *   </script>
 *
 * USO (Node.js):
 *   npm install ws node-fetch
 *   const B = require('./binance-v2')
 *   await B.start({ topN: 25, debug: true })
 *   B.on('update', s => console.log(B.getSummary('futures')))
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
    root.BinanceV2 = factory(WebSocket, fetch.bind(window))
  }
}(typeof globalThis !== 'undefined' ? globalThis : this, function (WSImpl, fetchFn) {
  'use strict'

  // ─── ENDPOINTS ──────────────────────────────────────────────

  const EP = {
    spot: {
      rest:    'https://api.binance.com/api/v3/ticker/24hr',
      depth:   'https://api.binance.com/api/v3/depth',
      wsBase:  'wss://stream.binance.com:9443/stream',
    },
    futures: {
      rest:    'https://fapi.binance.com/fapi/v1/ticker/24hr',
      exch:    'https://fapi.binance.com/fapi/v1/exchangeInfo',
      depth:   'https://fapi.binance.com/fapi/v1/depth',
      wsPub:   'wss://fstream.binance.com/public/stream',   // aggTrade, miniTicker, depth
      wsMkt:   'wss://fstream.binance.com/market/stream',   // markPrice
      wsApi:   'wss://ws-fapi.binance.com/ws-fapi/v1',      // request/response API
    },
  }

  // ─── DEFAULTS ───────────────────────────────────────────────

  const DEFAULTS = {
    topN:          25,
    spotQuote:     'USDC',
    futuresQuote:  'USDT',
    window1Ms:     60_000,    // janela deslizante 1 min
    window5Ms:     300_000,   // janela deslizante 5 min
    refreshMs:     30_000,    // re-rank REST
    reconnectMs:   3_500,
    updateHz:      1_000,     // evento 'update' periódico
    maxPerConn:    400,       // max streams por conexão WS
    excludeKw:     ['UP','DOWN','BULL','BEAR','3L','3S','LEVERAGE','HEDGE'],
    debug:         false,
  }

  // ─── ESTADO ─────────────────────────────────────────────────

  const S = {
    opts:      null,
    perps:     new Set(),
    symbols:   { spot: [], futures: [] },
    entries:   { spot: new Map(), futures: new Map() },
    conns:     {
      spot:    null,   // aggTrade + miniTicker
      futPub:  null,   // aggTrade + miniTicker
      futMkt:  null,   // markPrice
      futApi:  null,   // WS API (depth)
      obSpot:  null,   // spot depth stream
    },
    book: {
      sym:        null,
      mkt:        null,
      depth:      5,
      apiSeq:     0,
      apiPending: new Map(),
      pollTimer:  null,
    },
    listeners: new Map(),
    running:   false,
    timers:    [],
  }

  // ─── ENTRY ──────────────────────────────────────────────────
  /**
   * @typedef {Object} Entry
   * @property {string}  sym              símbolo
   * @property {string}  mkt              'spot' | 'futures'
   * @property {number}  rank
   * @property {number}  price
   * @property {number}  prevPrice
   * @property {number}  markPrice        futures only
   * @property {number}  fundingRate      futures only (decimal)
   * @property {number}  msToFunding      futures only
   * @property {number}  change24h        % 24h
   * @property {number}  vol24h           volume 24h (USDC/USDT)
   *
   * — Sessão —
   * @property {number}  sessBuy          vol comprador acumulado
   * @property {number}  sessSell         vol vendedor acumulado
   * @property {number}  sessCount        total trades
   * @property {number}  sessBuyImpact    impacto comprador no preço
   * @property {number}  sessSellImpact   impacto vendedor no preço
   *
   * — Janela 1 min —
   * @property {number}  w1Buy
   * @property {number}  w1Sell
   * @property {number}  w1Delta          buy - sell
   *
   * — Janela 5 min —
   * @property {number}  w5Buy
   * @property {number}  w5Sell
   *
   * — Derivados —
   * @property {number}  sessRatio        % buy sessão
   * @property {number}  w1Ratio          % buy 1min
   * @property {number}  w5Ratio          % buy 5min
   * @property {number}  pressure         −100…+100
   * @property {number}  impactRatio      % impacto compradores
   *
   * — Sinal sticky (só muda em extreme) —
   * @property {string}  sigKey           'extreme_buy' | 'extreme_sell' | 'off'
   * @property {string}  sigTxt           texto do sinal
   */
  function mkEntry(sym, mkt, rank, t) {
    return {
      sym, mkt, rank,
      price: +t.lastPrice, prevPrice: +t.lastPrice,
      markPrice: 0, fundingRate: 0, msToFunding: 0,
      change24h: +t.priceChangePercent, vol24h: +t.quoteVolume,
      sessBuy: 0, sessSell: 0, sessCount: 0,
      sessBuyImpact: 0, sessSellImpact: 0,
      w1Buy: 0, w1Sell: 0, w1Delta: 0, w1BI: 0, w1SI: 0,
      w5Buy: 0, w5Sell: 0,
      sessRatio: 50, w1Ratio: 50, w5Ratio: 50,
      pressure: 0, impactRatio: 50,
      sigKey: 'off', sigTxt: '—',   // STICKY — only updates on extreme
      lastTs: 0, _trades: [],
    }
  }

  // ─── REST ────────────────────────────────────────────────────

  async function get(url) {
    const r = await fetchFn(url)
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
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
      _log(`[perps] ${S.perps.size} perpétuos válidos`)
    } catch (err) {
      _log('[perps] erro:', err.message)
    }
  }

  async function loadRanking(mkt) {
    const isSpot = mkt === 'spot'
    const quote  = isSpot ? S.opts.spotQuote : S.opts.futuresQuote
    const data   = await get(EP[mkt].rest)

    const filtered = data
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
        ex.rank      = i + 1
        ex.change24h = +t.priceChangePercent
        ex.vol24h    = +t.quoteVolume
      } else {
        S.entries[mkt].set(t.symbol, mkEntry(t.symbol, mkt, i+1, t))
      }
    })

    _log(`[${mkt}] ranking: ${newSyms.length}`)
    emit('ranking', { mkt, symbols: newSyms })
  }

  // ─── PROCESSADORES ──────────────────────────────────────────

  /**
   * aggTrade — base de toda a análise buy/sell
   * m = false → BUY taker (comprador foi agressivo)
   * m = true  → SELL taker (vendedor foi agressivo)
   */
  function onAggTrade(mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return

    const price = +d.p, qty = +d.q, vol = price * qty
    const buy   = !d.m
    const ts    = d.T
    const pd    = e.lastTs > 0 ? price - e.price : 0

    e.prevPrice = e.price
    e.price     = price
    e.lastTs    = ts

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

    const prevKey = e.sigKey
    recompute(e, ts)

    emit('tick', snap(e))

    // Emitir 'extreme' apenas quando há transição para um estado extremo
    if (e.sigKey !== prevKey && e.sigKey !== 'off') {
      emit('extreme', snap(e))
    }
  }

  function onMarkPrice(d) {
    const e = S.entries.futures.get(d.s)
    if (!e) return
    e.markPrice    = +d.p
    e.fundingRate  = +d.r
    e.msToFunding  = +d.T - Date.now()
    emit('mark', snap(e))
  }

  function onMiniTicker(mkt, d) {
    const e = S.entries[mkt].get(d.s)
    if (!e) return
    e.prevPrice = e.price
    e.price     = +d.c
    e.vol24h    = +d.q
    if (+d.o > 0) e.change24h = (+d.c - +d.o) / +d.o * 100
  }

  // ─── RECOMPUTE ───────────────────────────────────────────────

  function recompute(e, nowTs) {
    const cut1 = nowTs - S.opts.window1Ms
    const cut5 = nowTs - S.opts.window5Ms
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

    // Pressão combinada -100..+100 (peso maior para janela recente)
    e.pressure = (e.w1Ratio-50)*2*.55 + (e.w5Ratio-50)*2*.30 + (e.sessRatio-50)*2*.15

    const ti = e.sessBuyImpact + e.sessSellImpact
    e.impactRatio = ti > 0 ? e.sessBuyImpact/ti*100 : 50

    // ── STICKY SIGNAL ───────────────────────────────────
    // O sinal só muda quando a pressão cruza os limiares extremos.
    // Não mostra estados intermédios: apenas extreme_buy / extreme_sell.
    const p = e.pressure
    if (p >= 72) {
      e.sigKey = 'extreme_buy'
      e.sigTxt = '🔥 COMPRA MAX'
    } else if (p <= -72) {
      e.sigKey = 'extreme_sell'
      e.sigTxt = '💀 VENDA MAX'
    }
    // else: mantém e.sigKey e e.sigTxt inalterados
  }

  // ─── SNAPSHOT ────────────────────────────────────────────────

  function snap(e) {
    const { _trades, ...out } = e
    return { ...out, _ts: Date.now() }
  }

  function getAll() {
    return {
      spot:    S.symbols.spot.map(s => S.entries.spot.get(s)).filter(Boolean).map(snap),
      futures: S.symbols.futures.map(s => S.entries.futures.get(s)).filter(Boolean).map(snap),
      ts:      Date.now(),
    }
  }

  // ─── WS FACTORY ─────────────────────────────────────────────

  function openWS(url, onMsg, id) {
    const ws = new WSImpl(url)
    ws.onopen    = () => { _log(`[WS ${id}] conectado`); emit('status', { id, status: 'connected' }) }
    ws.onmessage = ev => {
      try {
        const d = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString())
        onMsg(d.data || d)
      } catch(_) {}
    }
    ws.onerror   = () => emit('status', { id, status: 'error' })
    ws.onclose   = () => emit('status', { id, status: 'closed' })
    return ws
  }

  function withReconnect(connKey, buildFn, delay) {
    let closing = false
    const connect = () => {
      if (S.conns[connKey]) { S.conns[connKey]._closing = true; try { S.conns[connKey].ws.close() } catch(_){} }
      const ws = buildFn()
      ws.addEventListener('close', () => {
        if (!closing && !ws._closing) setTimeout(connect, delay || S.opts.reconnectMs)
      })
      S.conns[connKey] = { ws, close: () => { closing=true; ws._closing=true; try{ws.close()}catch(_){} } }
    }
    connect()
  }

  // ─── SPOT WS (aggTrade + miniTicker) ────────────────────────

  function openSpotWS() {
    const syms = S.symbols.spot
    const streams = syms.flatMap(s => [s.toLowerCase()+'@aggTrade', s.toLowerCase()+'@miniTicker'])
    const url = EP.spot.wsBase + '?streams=' + streams.join('/')
    withReconnect('spot', () => openWS(url, d => {
      if (!d?.e) return
      if (d.e === 'aggTrade')       onAggTrade('spot', d)
      if (d.e === '24hrMiniTicker') onMiniTicker('spot', d)
    }, 'spot'))
  }

  // ─── FUTURES PUBLIC WS (aggTrade + miniTicker) ──────────────

  function openFutPublicWS() {
    const syms = S.symbols.futures
    const streams = syms.flatMap(s => [s.toLowerCase()+'@aggTrade', s.toLowerCase()+'@miniTicker'])
    const url = EP.futures.wsPub + '?streams=' + streams.join('/')
    withReconnect('futPub', () => openWS(url, d => {
      if (!d?.e) return
      if (d.e === 'aggTrade')       onAggTrade('futures', d)
      if (d.e === '24hrMiniTicker') onMiniTicker('futures', d)
    }, 'fut-pub'))
  }

  // ─── FUTURES MARKET WS (markPrice) ──────────────────────────
  // Requer rota /market — não disponível no /stream genérico

  function openFutMarketWS() {
    const syms    = S.symbols.futures
    const streams = syms.map(s => s.toLowerCase()+'@markPrice@1s')
    const url     = EP.futures.wsMkt + '?streams=' + streams.join('/')
    withReconnect('futMkt', () => openWS(url, d => {
      if (d?.e === 'markPriceUpdate') onMarkPrice(d)
    }, 'fut-mkt'))
  }

  // ─── FUTURES WebSocket API ───────────────────────────────────
  //
  // wss://ws-fapi.binance.com/ws-fapi/v1
  //
  // Protocolo request/response:
  //   Envio:   { "id": "string", "method": "depth", "params": { "symbol": "BTCUSDT", "limit": 10 } }
  //   Resposta:{ "id": "string", "status": 200, "result": { "bids": [...], "asks": [...] } }
  //
  // Usado para obter snapshots de depth a pedido (em vez de stream contínuo),
  // pois a ws-fapi não suporta subscrições — é exclusivamente request/response.

  function openFutApiWS() {
    if (S.conns.futApi) { try { S.conns.futApi.ws.close() } catch(_){} }

    const ws = new WSImpl(EP.futures.wsApi)
    S.conns.futApi = { ws }

    ws.onopen = () => {
      _log('[ws-fapi] conectado')
      emit('status', { id: 'fut-api', status: 'connected' })
      // Se o book está aberto em futures, arrancar o poll
      if (S.book.sym && S.book.mkt === 'futures') _startFutApiPoll()
    }

    ws.onmessage = ev => {
      try {
        const msg = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString())
        // Resposta normal: { id, status, result }
        const cb = S.book.apiPending.get(msg.id)
        if (cb) {
          S.book.apiPending.delete(msg.id)
          if (msg.status === 200) cb(null, msg.result)
          else cb(new Error(msg.error?.msg || 'ws-fapi error ' + msg.status))
        }
      } catch(_) {}
    }

    ws.onerror = () => emit('status', { id: 'fut-api', status: 'error' })
    ws.onclose = () => {
      // Resolver pendentes com erro
      S.book.apiPending.forEach(cb => cb(new Error('ws-fapi closed')))
      S.book.apiPending.clear()
      emit('status', { id: 'fut-api', status: 'closed' })
      setTimeout(openFutApiWS, S.opts.reconnectMs)
    }
  }

  /**
   * Envia um pedido à ws-fapi e devolve uma Promise com o resultado.
   * @param {string} method   ex: 'depth'
   * @param {Object} params   ex: { symbol: 'BTCUSDT', limit: 10 }
   * @returns {Promise<Object>}
   */
  function futApiRequest(method, params) {
    return new Promise((resolve, reject) => {
      const ws = S.conns.futApi?.ws
      if (!ws || ws.readyState !== 1) {
        reject(new Error('ws-fapi não conectado'))
        return
      }
      const id = String(++S.book.apiSeq)
      const timeout = setTimeout(() => {
        S.book.apiPending.delete(id)
        reject(new Error('ws-fapi timeout'))
      }, 6000)
      S.book.apiPending.set(id, (err, result) => {
        clearTimeout(timeout)
        err ? reject(err) : resolve(result)
      })
      ws.send(JSON.stringify({ id, method, params }))
    })
  }

  function _startFutApiPoll() {
    if (S.book.pollTimer) clearInterval(S.book.pollTimer)
    const fetch = async () => {
      if (!S.book.sym || S.book.mkt !== 'futures') {
        clearInterval(S.book.pollTimer)
        return
      }
      try {
        const result = await futApiRequest('depth', {
          symbol: S.book.sym,
          limit:  S.book.depth,
        })
        _emitBook(result.bids, result.asks)
      } catch (_) {
        // Fallback: REST depth
        try {
          const r = await fetchFn(`${EP.futures.depth}?symbol=${S.book.sym}&limit=${S.book.depth}`)
          const d = await r.json()
          _emitBook(d.bids, d.asks)
        } catch(_) {}
      }
    }
    fetch()
    S.book.pollTimer = setInterval(fetch, 500)
  }

  // ─── ORDER BOOK (spot) ───────────────────────────────────────
  // Para spot usa stream @depth{N}@100ms contínuo

  function _openSpotBookWS(sym, depth) {
    if (S.conns.obSpot) { try { S.conns.obSpot.ws.close() } catch(_){} }
    const url = EP.spot.wsBase + '?streams=' + sym.toLowerCase() + '@depth' + depth + '@100ms'
    const ws  = new WSImpl(url)
    S.conns.obSpot = { ws }
    ws.onopen    = () => emit('status', { id: 'ob-spot', status: 'connected' })
    ws.onmessage = ev => {
      try {
        const d = JSON.parse(typeof ev.data==='string' ? ev.data : ev.data.toString())
        const raw = d.data || d
        const bids = raw.bids || raw.b
        const asks = raw.asks || raw.a
        if (bids && asks) _emitBook(bids, asks)
      } catch(_) {}
    }
    ws.onerror   = () => emit('status', { id: 'ob-spot', status: 'error' })
    ws.onclose   = () => {
      emit('status', { id: 'ob-spot', status: 'closed' })
      if (S.book.sym === sym && S.book.mkt === 'spot') setTimeout(() => _openSpotBookWS(sym, depth), 3000)
    }
  }

  // ─── EMIT BOOK ───────────────────────────────────────────────

  function _emitBook(rawBids, rawAsks) {
    if (!S.book.sym || !S.book.mkt) return

    const e = S.entries[S.book.mkt].get(S.book.sym)

    const bids = [...rawBids].map(r => ({ p:+r[0], q:+r[1] })).sort((a,b)=>b.p-a.p)
    const asks = [...rawAsks].map(r => ({ p:+r[0], q:+r[1] })).sort((a,b)=>a.p-b.p)

    const bestBid   = bids[0]?.p || 0
    const bestAsk   = asks[0]?.p || 0
    const spread    = bestBid && bestAsk ? bestAsk - bestBid : 0
    const spreadPct = bestBid ? spread/bestBid*100 : 0
    const maxQty    = Math.max(...bids.map(r=>r.q), ...asks.map(r=>r.q), 1)

    let bidCum=0, askCum=0
    bids.forEach(r => { bidCum += r.p*r.q; r.total=bidCum; r.depthPct=r.q/maxQty*100 })
    asks.forEach(r => { askCum += r.p*r.q; r.total=askCum; r.depthPct=r.q/maxQty*100 })

    emit('book', {
      sym: S.book.sym, mkt: S.book.mkt, depth: S.book.depth,
      bids, asks, bestBid, bestAsk, spread, spreadPct, maxQty,
      lastPrice:   e?.price || 0,
      markPrice:   e?.markPrice || 0,
      fundingRate: e?.fundingRate || 0,
      ts: Date.now(),
    })
  }

  // ─── SUBSCRIBE BOOK (API pública) ───────────────────────────

  /**
   * Subscreve o order book de um símbolo.
   * @param {string} sym    ex: 'BTCUSDC', 'BTCUSDT'
   * @param {string} mkt    'spot' | 'futures'
   * @param {number} depth  5 | 10 | 20
   */
  function subscribeBook(sym, mkt, depth = 5) {
    // Parar subscrição anterior
    if (S.book.pollTimer) { clearInterval(S.book.pollTimer); S.book.pollTimer = null }
    if (S.conns.obSpot)   { try { S.conns.obSpot.ws.close() } catch(_){} }

    S.book.sym   = sym
    S.book.mkt   = mkt
    S.book.depth = depth

    if (mkt === 'futures') {
      // Usar ws-fapi request/response com polling 500ms
      if (S.conns.futApi?.ws?.readyState === 1) {
        _startFutApiPoll()
      } else {
        // ws-fapi a reconectar — quando abrir vai arrancar o poll automaticamente
        emit('status', { id: 'ob-fut', status: 'connecting' })
      }
    } else {
      // Spot: stream contínuo @depth{N}@100ms
      _openSpotBookWS(sym, depth)
    }

    emit('status', { id: 'book', status: 'subscribed', sym, mkt, depth })
  }

  function unsubscribeBook() {
    if (S.book.pollTimer) { clearInterval(S.book.pollTimer); S.book.pollTimer = null }
    if (S.conns.obSpot)   { try { S.conns.obSpot.ws.close() } catch(_){} }
    S.book.sym = null; S.book.mkt = null
    emit('status', { id: 'book', status: 'unsubscribed' })
  }

  // ─── SUMMARY ────────────────────────────────────────────────

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
    const sessRatio   = tv>0 ? tB/tv*100 : 50
    const impactRatio = ti>0 ? tBI/ti*100 : 50
    const w1Ratio     = tw>0 ? w1B/tw*100 : 50
    const byW1 = [...entries].sort((a,b)=>Math.abs(b.w1Delta)-Math.abs(a.w1Delta))
    const byP  = [...entries].sort((a,b)=>Math.abs(b.pressure)-Math.abs(a.pressure))
    const extr = entries.filter(e => e.sigKey !== 'off')
    return {
      mkt, sessRatio, impactRatio, w1Ratio,
      totalBuyVol: tB, totalSellVol: tS, totalVol: tv,
      delta: tB-tS, w1Delta: w1B-w1S,
      dominantSide:  sessRatio>=50 ? 'buy' : 'sell',
      priceMover:    impactRatio>=50 ? 'buyers' : 'sellers',
      topW1Mover:    byW1[0] ? snap(byW1[0]) : null,
      topPressure:   byP[0]  ? snap(byP[0])  : null,
      extremeSignals: extr.map(snap),
      count: entries.length, ts: Date.now(),
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
    ls.forEach(fn => { try { fn(data) } catch(e) { console.error('[BinanceV2]', e) } })
  }

  // ─── START / STOP ─────────────────────────────────────────────

  /**
   * @param {Object} [opts]
   * @param {number}   [opts.topN=25]
   * @param {string}   [opts.spotQuote='USDC']
   * @param {string}   [opts.futuresQuote='USDT']
   * @param {number}   [opts.window1Ms=60000]
   * @param {number}   [opts.window5Ms=300000]
   * @param {number}   [opts.refreshMs=30000]
   * @param {boolean}  [opts.debug=false]
   */
  async function start(opts = {}) {
    if (S.running) { console.warn('[BinanceV2] já em execução'); return }
    S.opts    = { ...DEFAULTS, ...opts }
    S.running = true
    _log('iniciando...', S.opts)

    await loadPerps()
    await Promise.all(['spot','futures'].map(loadRanking))

    openSpotWS()
    openFutPublicWS()
    openFutMarketWS()
    openFutApiWS()

    S.timers.push(setInterval(() => emit('update', getAll()), S.opts.updateHz))

    S.timers.push(setInterval(async () => {
      try {
        await loadPerps()
        const ops = ['spot','futures'].map(async mkt => {
          const old = S.symbols[mkt].join()
          await loadRanking(mkt)
          if (S.symbols[mkt].join() !== old) {
            if (mkt === 'spot') openSpotWS()
            else { openFutPublicWS(); openFutMarketWS() }
          }
        })
        await Promise.all(ops)
      } catch (err) { _log('[refresh]', err.message) }
    }, S.opts.refreshMs))

    _log('iniciado ✓')
  }

  function stop() {
    S.running = false
    S.timers.forEach(t => clearInterval(t)); S.timers = []
    Object.values(S.conns).forEach(c => { if (c) try { c.ws?.close() || c.close?.() } catch(_){} })
    unsubscribeBook()
    emit('status', { id: 'all', status: 'stopped' })
    _log('parado ✓')
  }

  async function restart(opts) {
    stop(); S.running=false
    S.entries = { spot: new Map(), futures: new Map() }
    S.symbols = { spot: [], futures: [] }
    await new Promise(r => setTimeout(r, 600))
    await start(opts || S.opts)
  }

  // ─── UTILITÁRIOS ─────────────────────────────────────────────

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

  function fmtPct(n, d=2) { return (n>=0?'+':'')+n.toFixed(d)+'%' }
  function fmtFunding(r)   { return (r*100).toFixed(4)+'%' }
  function fmtCountdown(ms) {
    if (!ms||ms<=0) return '—'
    const h=Math.floor(ms/3600000), m=Math.floor((ms%3600000)/60000), s=Math.floor((ms%60000)/1000)
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`
  }

  function _log(...a) { if (S.opts?.debug) console.log('[BinanceV2]', ...a) }

  // ─── API ─────────────────────────────────────────────────────

  return {
    start, stop, restart,
    on, off,
    getAll, getSummary,
    getEntry: (sym, mkt='spot') => { const e=S.entries[mkt].get(sym); return e?snap(e):null },
    subscribeBook, unsubscribeBook,
    futApiRequest,
    fmtVol, fmtPrice, fmtPct, fmtFunding, fmtCountdown,
    get symbols()  { return { spot:[...S.symbols.spot], futures:[...S.symbols.futures] } },
    get running()  { return S.running },
    get version()  { return '4.0.0' },
  }
}))
