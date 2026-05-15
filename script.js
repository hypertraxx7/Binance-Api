/**
 * binance-ws.js
 * ═══════════════════════════════════════════════════════════════
 * Recolha de dados em tempo real via WebSocket — Binance API
 *
 * Suporta:
 *   • Top N Spot   USDC  — aggTrade + bookTicker + miniTicker
 *   • Top N Futures USDT — aggTrade + bookTicker + miniTicker
 *
 * Métricas calculadas por símbolo:
 *   • Volume comprador vs vendedor (USDC/USDT) — sessão + janela deslizante
 *   • Impacto no preço por lado (buyers vs sellers)
 *   • Pressão de compra/venda em tempo real
 *   • Delta de fluxo (1 min, 5 min, sessão)
 *   • Bid/Ask spread em tempo real
 *
 * Compatível com:
 *   • Node.js  — npm install ws
 *   • Browser  — usar directamente (usa WebSocket nativo)
 *
 * Uso (Node.js):
 *   const bws = require('./binance-ws')
 *   await bws.start({ topN: 25, markets: ['spot','futures'] })
 *   bws.on('tick',   data => console.log(data))
 *   bws.on('update', data => console.log(data))
 *
 * Uso (Browser — módulo ES):
 *   import * as bws from './binance-ws.js'
 *   await bws.start({ topN: 25 })
 *   bws.on('tick', data => renderUI(data))
 * ═══════════════════════════════════════════════════════════════
 */

;(function (root, factory) {
  // UMD — funciona em Node.js (CommonJS), browser (global) e bundlers (AMD)
  if (typeof module !== 'undefined' && module.exports) {
    const WS = (typeof WebSocket !== 'undefined') ? WebSocket : require('ws')
    module.exports = factory(WS)
  } else if (typeof define === 'function' && define.amd) {
    define([], () => factory(WebSocket))
  } else {
    root.BinanceWS = factory(WebSocket)
  }
}(typeof globalThis !== 'undefined' ? globalThis : this, function (WS) {
  'use strict'

  // ─────────────────────────────────────────────────────────────
  // DEFAULTS
  // ─────────────────────────────────────────────────────────────
  const DEFAULTS = {
    topN:            25,          // quantos símbolos por mercado
    markets:         ['spot', 'futures'],
    quoteAsset:      'USDT',       // futuras — mantido para compatibilidade
    spotQuote:       'USDC',       // spot usa USDC
    futuresQuote:    'USDT',       // futures usa USDT
    windowMs:        60_000,      // janela deslizante curta (1 min)
    windowMs5:       300_000,     // janela deslizante longa (5 min)
    refreshMs:       30_000,      // re-rank via REST
    reconnectMs:     3_000,       // atraso de reconexão WS
    maxReconnects:   0,           // 0 = ilimitado
    excludeKeywords: ['UP','DOWN','BULL','BEAR','3L','3S','LEVERAGE','HEDGE'],
    debug:           false,
  }

  // Endpoints
  const ENDPOINTS = {
    spot: {
      rest:      'https://api.binance.com/api/v3/ticker/24hr',
      streamBase:'wss://stream.binance.com:9443/stream',
    },
    futures: {
      rest:      'https://fapi.binance.com/fapi/v1/ticker/24hr',
      streamBase:'wss://fstream.binance.com/stream',
    },
  }

  // ─────────────────────────────────────────────────────────────
  // ESTADO INTERNO
  // ─────────────────────────────────────────────────────────────
  /**
   * symbols   — Map<market, string[]>           lista ordenada por rank
   * entries   — Map<market, Map<sym, Entry>>     dados por símbolo
   * sockets   — Map<market+streamType, WSConn>  conexões activas
   * listeners — Map<event, Set<fn>>             callbacks
   * opts      — opções mescladas com DEFAULTS
   * running   — boolean
   */
  const _state = {
    symbols:   { spot: [], futures: [] },
    entries:   { spot: new Map(), futures: new Map() },
    sockets:   new Map(),
    listeners: new Map(),
    opts:      null,
    running:   false,
    refreshTimer: null,
  }

  // ─────────────────────────────────────────────────────────────
  // ESTRUTURA DE UMA ENTRY
  // ─────────────────────────────────────────────────────────────
  /**
   * @typedef {Object} Entry
   * @property {string}   symbol
   * @property {string}   market          'spot' | 'futures'
   * @property {number}   rank            posição no ranking de volume
   * @property {number}   price           último preço negociado
   * @property {number}   prevPrice       preço anterior (para flash/delta)
   * @property {number}   priceOpen       preço de abertura 24h
   * @property {number}   priceHigh       máximo 24h
   * @property {number}   priceLow        mínimo 24h
   * @property {number}   change24h       variação % 24h
   * @property {number}   vol24hBase      volume 24h (base asset)
   * @property {number}   vol24hQuote     volume 24h (USDC para spot, USDT para futures)
   * @property {number}   bidPrice        melhor compra (order book)
   * @property {number}   askPrice        melhor venda  (order book)
   * @property {number}   spread          spread bid/ask absoluto
   * @property {number}   spreadPct       spread bid/ask em %
   *
   * — Sessão (desde início) —
   * @property {number}   sessionBuyQty   quantidade comprada (base)
   * @property {number}   sessionSellQty  quantidade vendida  (base)
   * @property {number}   sessionBuyVol   volume comprador   (USDC ou USDT conforme mercado)
   * @property {number}   sessionSellVol  volume vendedor    (USDC ou USDT conforme mercado)
   * @property {number}   sessionTrades   nº total de trades
   * @property {number}   sessionBuyImpact   impacto comprador no preço (ponderado por vol)
   * @property {number}   sessionSellImpact  impacto vendedor  no preço
   *
   * — Janela 1 min —
   * @property {number}   win1BuyVol
   * @property {number}   win1SellVol
   * @property {number}   win1Delta       buy - sell (USDC ou USDT)
   * @property {number}   win1BuyImpact
   * @property {number}   win1SellImpact
   * @property {number}   win1Trades
   *
   * — Janela 5 min —
   * @property {number}   win5BuyVol
   * @property {number}   win5SellVol
   * @property {number}   win5Delta
   * @property {number}   win5Trades
   *
   * — Derivados —
   * @property {number}   buyRatio        % de compra na sessão (0-100)
   * @property {number}   win1BuyRatio    % de compra na janela 1min
   * @property {number}   win5BuyRatio    % de compra na janela 5min
   * @property {number}   pressure        pressão combinada -100 a +100
   * @property {number}   impactRatio     % de impacto dos compradores (0-100)
   * @property {string}   signal          classificação textual
   * @property {string}   signalKey       'extreme_buy'|'strong_buy'|'buy'|'neutral'|'sell'|'strong_sell'|'extreme_sell'
   * @property {number}   lastTs          timestamp do último trade
   * @property {Array}    _trades         array de trades (janela deslizante interna)
   */
  function createEntry(symbol, market, rank, ticker) {
    return {
      symbol,
      market,
      rank,
      price:        +ticker.lastPrice,
      prevPrice:    +ticker.lastPrice,
      priceOpen:    +ticker.openPrice,
      priceHigh:    +ticker.highPrice,
      priceLow:     +ticker.lowPrice,
      change24h:    +ticker.priceChangePercent,
      vol24hBase:   +ticker.volume,
      vol24hQuote:  +ticker.quoteVolume,
      bidPrice:     0,
      askPrice:     0,
      spread:       0,
      spreadPct:    0,

      sessionBuyQty:     0,
      sessionSellQty:    0,
      sessionBuyVol:     0,
      sessionSellVol:    0,
      sessionTrades:     0,
      sessionBuyImpact:  0,
      sessionSellImpact: 0,

      win1BuyVol:    0,
      win1SellVol:   0,
      win1Delta:     0,
      win1BuyImpact: 0,
      win1SellImpact:0,
      win1Trades:    0,

      win5BuyVol:    0,
      win5SellVol:   0,
      win5Delta:     0,
      win5Trades:    0,

      buyRatio:      50,
      win1BuyRatio:  50,
      win5BuyRatio:  50,
      pressure:      0,
      impactRatio:   50,
      signal:        'NEUTRO',
      signalKey:     'neutral',
      lastTs:        0,

      _trades:       [],     // { ts, qty, vol, isBuy, priceDelta }
    }
  }

  // ─────────────────────────────────────────────────────────────
  // FETCH REST
  // ─────────────────────────────────────────────────────────────
  async function fetchTicker(market) {
    const url = ENDPOINTS[market].rest
    const res  = await fetch(url)
    if (!res.ok) throw new Error(`REST ${market} HTTP ${res.status}`)
    return res.json()
  }

  async function loadRanking(market) {
    const opts   = _state.opts
    const map    = _state.entries[market]
    const tickers = await fetchTicker(market)

    // Spot usa USDC, Futures usa USDT
    const quote = market === 'spot'
      ? (opts.spotQuote    || 'USDC')
      : (opts.futuresQuote || 'USDT')

    const filtered = tickers
      .filter(t =>
        t.symbol.endsWith(quote) &&
        !opts.excludeKeywords.some(k => t.symbol.includes(k))
      )
      .sort((a, b) => +b.quoteVolume - +a.quoteVolume)
      .slice(0, opts.topN)

    const newSyms = filtered.map(t => t.symbol)
    _state.symbols[market] = newSyms

    filtered.forEach((t, i) => {
      const rank   = i + 1
      const exists = map.get(t.symbol)
      if (exists) {
        // Actualizar só campos REST sem apagar acumuladores de sessão
        exists.rank       = rank
        exists.change24h  = +t.priceChangePercent
        exists.vol24hBase = +t.volume
        exists.vol24hQuote= +t.quoteVolume
        exists.priceHigh  = +t.highPrice
        exists.priceLow   = +t.lowPrice
        exists.priceOpen  = +t.openPrice
      } else {
        map.set(t.symbol, createEntry(t.symbol, market, rank, t))
      }
    })

    _log(`[REST] ${market} ranking loaded: ${newSyms.length} symbols`)
    emit('ranking', { market, symbols: newSyms })
  }

  // ─────────────────────────────────────────────────────────────
  // PROCESSAR MENSAGENS WEBSOCKET
  // ─────────────────────────────────────────────────────────────

  /**
   * aggTrade — trade individual (mais granular)
   * Campos relevantes:
   *   s  = symbol
   *   p  = price           (string)
   *   q  = quantity        (string)
   *   m  = isBuyerMaker
   *        false → comprador foi agressivo (BUY taker)
   *        true  → vendedor foi agressivo  (SELL taker)
   *   T  = timestamp ms
   */
  function handleAggTrade(market, d) {
    const map   = _state.entries[market]
    const entry = map.get(d.s)
    if (!entry) return

    const price    = +d.p
    const qty      = +d.q
    const vol      = price * qty          // volume em USDC (spot) ou USDT (futures)
    const isBuy    = !d.m                 // false = buyer taker = BUY
    const ts       = d.T
    const opts     = _state.opts

    // Delta de preço desde o último trade conhecido
    const priceDelta = (entry.price > 0 && entry.lastTs > 0)
      ? price - entry.price
      : 0

    // ── Actualizar preço ────────────────────────────────────
    entry.prevPrice = entry.price
    entry.price     = price
    entry.lastTs    = ts

    // ── Acumuladores de sessão ──────────────────────────────
    if (isBuy) {
      entry.sessionBuyQty += qty
      entry.sessionBuyVol += vol
    } else {
      entry.sessionSellQty += qty
      entry.sessionSellVol += vol
    }
    entry.sessionTrades++

    // ── Impacto no preço ────────────────────────────────────
    // Apenas conta quando o preço se move na direcção do agressor:
    //   BUY  + preço subiu  → compradores moveram o preço
    //   SELL + preço caiu   → vendedores moveram o preço
    const absDelta = Math.abs(priceDelta)
    if (isBuy  && priceDelta > 0) entry.sessionBuyImpact  += absDelta * vol
    if (!isBuy && priceDelta < 0) entry.sessionSellImpact += absDelta * vol

    // ── Trade para janelas deslizantes ──────────────────────
    const trade = { ts, qty, vol, isBuy, priceDelta }
    entry._trades.push(trade)

    // Purgar trades antigos (além de 5 min)
    const cutoff5 = ts - opts.windowMs5
    while (entry._trades.length && entry._trades[0].ts < cutoff5) {
      entry._trades.shift()
    }

    // ── Derivados ───────────────────────────────────────────
    _recompute(entry, ts)

    // ── Emitir evento de tick ───────────────────────────────
    emit('tick', _snapshot(entry))
  }

  /**
   * bookTicker — melhor bid/ask em tempo real
   * Campos: s, b (bidPrice), B (bidQty), a (askPrice), A (askQty)
   */
  function handleBookTicker(market, d) {
    const entry = _state.entries[market].get(d.s)
    if (!entry) return

    const bid = +d.b
    const ask = +d.a
    entry.bidPrice  = bid
    entry.askPrice  = ask
    entry.spread    = ask - bid
    entry.spreadPct = bid > 0 ? ((ask - bid) / bid) * 100 : 0
  }

  /**
   * miniTicker — ticker resumido (actualiza preço e 24h)
   * Campos: s, c (close/last), o (open), h (high), l (low), v (vol base), q (vol quote)
   */
  function handleMiniTicker(market, d) {
    const entry = _state.entries[market].get(d.s)
    if (!entry) return

    entry.prevPrice   = entry.price
    entry.price       = +d.c
    entry.priceOpen   = +d.o
    entry.priceHigh   = +d.h
    entry.priceLow    = +d.l
    entry.vol24hBase  = +d.v
    entry.vol24hQuote = +d.q

    const open = +d.o
    if (open > 0) {
      entry.change24h = ((+d.c - open) / open) * 100
    }
  }

  // ─────────────────────────────────────────────────────────────
  // RECOMPUTAR DERIVADOS
  // ─────────────────────────────────────────────────────────────
  function _recompute(entry, nowTs) {
    const opts = _state.opts
    const cut1 = nowTs - opts.windowMs
    const cut5 = nowTs - opts.windowMs5

    // Somar janelas
    let b1v=0, s1v=0, b1i=0, s1i=0, t1=0
    let b5v=0, s5v=0, t5=0

    for (const tr of entry._trades) {
      const in5 = tr.ts >= cut5
      const in1 = tr.ts >= cut1

      if (in5) {
        if (tr.isBuy) b5v += tr.vol; else s5v += tr.vol
        t5++
      }
      if (in1) {
        if (tr.isBuy) { b1v += tr.vol; if (tr.priceDelta > 0) b1i += Math.abs(tr.priceDelta) * tr.vol }
        else           { s1v += tr.vol; if (tr.priceDelta < 0) s1i += Math.abs(tr.priceDelta) * tr.vol }
        t1++
      }
    }

    entry.win1BuyVol    = b1v
    entry.win1SellVol   = s1v
    entry.win1Delta     = b1v - s1v
    entry.win1BuyImpact = b1i
    entry.win1SellImpact= s1i
    entry.win1Trades    = t1

    entry.win5BuyVol    = b5v
    entry.win5SellVol   = s5v
    entry.win5Delta     = b5v - s5v
    entry.win5Trades    = t5

    // Ratios
    const totSess = entry.sessionBuyVol + entry.sessionSellVol
    const tot1    = b1v + s1v
    const tot5    = b5v + s5v

    entry.buyRatio     = totSess > 0 ? (entry.sessionBuyVol / totSess) * 100 : 50
    entry.win1BuyRatio = tot1    > 0 ? (b1v / tot1) * 100 : 50
    entry.win5BuyRatio = tot5    > 0 ? (b5v / tot5) * 100 : 50

    // Pressão combinada: peso maior para janela recente
    // Resultado: -100 (venda extrema) a +100 (compra extrema)
    const p1 = (entry.win1BuyRatio - 50) * 2   // -100..+100
    const p5 = (entry.win5BuyRatio - 50) * 2
    const ps = (entry.buyRatio      - 50) * 2
    entry.pressure = p1 * 0.50 + p5 * 0.30 + ps * 0.20

    // Impacto no preço
    const totImpact = entry.sessionBuyImpact + entry.sessionSellImpact
    entry.impactRatio = totImpact > 0
      ? (entry.sessionBuyImpact / totImpact) * 100
      : 50

    // Sinal
    const pr = entry.pressure
    if      (pr >=  70) { entry.signalKey = 'extreme_buy';   entry.signal = '🔥🔥 COMPRA EXTREMA'  }
    else if (pr >=  35) { entry.signalKey = 'strong_buy';    entry.signal = '🔥 COMPRA FORTE'      }
    else if (pr >=  10) { entry.signalKey = 'buy';           entry.signal = '↑ COMPRADORES'        }
    else if (pr >=  -10){ entry.signalKey = 'neutral';       entry.signal = '⚖ NEUTRO'             }
    else if (pr >= -35) { entry.signalKey = 'sell';          entry.signal = '↓ VENDEDORES'         }
    else if (pr >= -70) { entry.signalKey = 'strong_sell';   entry.signal = '📉 VENDA FORTE'       }
    else                 { entry.signalKey = 'extreme_sell';  entry.signal = '💀 VENDA EXTREMA'    }
  }

  // ─────────────────────────────────────────────────────────────
  // SNAPSHOT — cópia limpa de uma entry para emitir
  // ─────────────────────────────────────────────────────────────
  function _snapshot(entry) {
    const { _trades, ...clean } = entry   // remover array interno
    return Object.freeze({ ...clean })
  }

  // ─────────────────────────────────────────────────────────────
  // WEBSOCKET — GESTÃO DE CONEXÕES
  // ─────────────────────────────────────────────────────────────

  /**
   * Abre uma conexão de stream combinado.
   * @param {string} market  'spot' | 'futures'
   * @param {string} type    'trade' | 'book' | 'mini'
   * @param {string[]} syms  lista de símbolos
   */
  function openStream(market, type, syms) {
    if (!syms.length) return

    const key = `${market}:${type}`
    const existing = _state.sockets.get(key)
    if (existing) {
      existing._closing = true
      try { existing.ws.close() } catch(_) {}
    }

    const streamNames = syms.map(s => {
      const sl = s.toLowerCase()
      if (type === 'trade') return sl + '@aggTrade'
      if (type === 'book')  return sl + '@bookTicker'
      if (type === 'mini')  return sl + '@miniTicker'
    })

    const url = ENDPOINTS[market].streamBase + '?streams=' + streamNames.join('/')
    const conn = { ws: null, _closing: false, reconnects: 0 }
    _state.sockets.set(key, conn)

    function connect() {
      _log(`[WS] open ${key} (${syms.length} streams)`)
      const ws = new WS(url)
      conn.ws  = ws

      ws.onopen = () => {
        _log(`[WS] connected ${key}`)
        conn.reconnects = 0
        emit('status', { market, type, status: 'connected', key })
        _checkAllConnected()
      }

      ws.onmessage = e => {
        let msg
        try { msg = JSON.parse(typeof e.data === 'string' ? e.data : e.data.toString()) }
        catch { return }

        // Combined stream: { stream, data }
        const data = msg.data || msg

        if (!data || !data.e) return
        switch (data.e) {
          case 'aggTrade':  handleAggTrade(market, data);   break
          case 'bookTicker': handleBookTicker(market, data); break
          case '24hrMiniTicker': handleMiniTicker(market, data); break
        }
      }

      ws.onerror = err => {
        _log(`[WS] error ${key}:`, err?.message || err)
        emit('status', { market, type, status: 'error', key })
      }

      ws.onclose = () => {
        if (conn._closing) return
        const delay = _state.opts.reconnectMs
        const maxR  = _state.opts.maxReconnects
        if (maxR > 0 && conn.reconnects >= maxR) {
          _log(`[WS] max reconnects reached ${key}`)
          emit('status', { market, type, status: 'failed', key })
          return
        }
        conn.reconnects++
        _log(`[WS] closed ${key} — reconnect #${conn.reconnects} in ${delay}ms`)
        emit('status', { market, type, status: 'reconnecting', key, attempt: conn.reconnects })
        setTimeout(connect, delay)
      }
    }

    connect()
  }

  function _checkAllConnected() {
    const all = [..._state.sockets.values()]
    const allOpen = all.length > 0 && all.every(c => c.ws?.readyState === 1)
    if (allOpen) emit('ready', _getAllData())
  }

  function _openStreamsForMarket(market) {
    const syms = _state.symbols[market]
    openStream(market, 'trade', syms)   // aggTrade — principal (buy/sell)
    openStream(market, 'book',  syms)   // bookTicker — spread
    openStream(market, 'mini',  syms)   // miniTicker — preço/24h
  }

  // ─────────────────────────────────────────────────────────────
  // EVENTOS (pub/sub simples)
  // ─────────────────────────────────────────────────────────────

  /**
   * Eventos disponíveis:
   *   'ready'    — emitido quando todos os WS estão conectados. Payload: snapshot completo
   *   'tick'     — emitido a cada aggTrade. Payload: Entry snapshot
   *   'update'   — emitido a cada 1s com snapshot de todos os símbolos
   *   'ranking'  — emitido quando o ranking é (re)carregado via REST
   *   'status'   — mudanças de estado das conexões WS
   */
  function on(event, fn) {
    if (!_state.listeners.has(event)) _state.listeners.set(event, new Set())
    _state.listeners.get(event).add(fn)
    return () => off(event, fn)   // retorna função para remover listener
  }

  function off(event, fn) {
    _state.listeners.get(event)?.delete(fn)
  }

  function emit(event, data) {
    const listeners = _state.listeners.get(event)
    if (!listeners?.size) return
    listeners.forEach(fn => {
      try { fn(data) }
      catch (err) { console.error(`[BinanceWS] listener error (${event}):`, err) }
    })
  }

  // ─────────────────────────────────────────────────────────────
  // API PÚBLICA — DADOS
  // ─────────────────────────────────────────────────────────────

  /** Retorna snapshot de todos os símbolos de um mercado */
  function getMarket(market) {
    const syms = _state.symbols[market]
    const map  = _state.entries[market]
    return syms
      .map(s => map.get(s))
      .filter(Boolean)
      .map(_snapshot)
  }

  /** Retorna snapshot de um símbolo específico */
  function getSymbol(symbol, market = 'spot') {
    const entry = _state.entries[market].get(symbol)
    return entry ? _snapshot(entry) : null
  }

  /** Retorna snapshot completo de todos os mercados */
  function _getAllData() {
    return {
      spot:    getMarket('spot'),
      futures: getMarket('futures'),
      ts:      Date.now(),
    }
  }

  /**
   * Retorna métricas agregadas de um mercado
   * (totais, quem domina, top movers, etc.)
   */
  function getMarketSummary(market) {
    const entries = getMarket(market)
    if (!entries.length) return null

    let totalBuyVol = 0, totalSellVol = 0
    let totalBuyImpact = 0, totalSellImpact = 0
    let totalWin1Buy = 0, totalWin1Sell = 0

    entries.forEach(e => {
      totalBuyVol    += e.sessionBuyVol
      totalSellVol   += e.sessionSellVol
      totalBuyImpact += e.sessionBuyImpact
      totalSellImpact+= e.sessionSellImpact
      totalWin1Buy   += e.win1BuyVol
      totalWin1Sell  += e.win1SellVol
    })

    const totalVol    = totalBuyVol + totalSellVol
    const totalImpact = totalBuyImpact + totalSellImpact
    const totalWin1   = totalWin1Buy + totalWin1Sell

    const buyRatio    = totalVol    > 0 ? totalBuyVol    / totalVol    * 100 : 50
    const impactRatio = totalImpact > 0 ? totalBuyImpact / totalImpact * 100 : 50
    const win1Ratio   = totalWin1   > 0 ? totalWin1Buy   / totalWin1   * 100 : 50

    // Top movers — maior delta absoluto na janela 1 min
    const sorted = [...entries].sort((a, b) => Math.abs(b.win1Delta) - Math.abs(a.win1Delta))
    const topMoverBuy  = sorted.find(e => e.win1Delta > 0) || sorted[0]
    const topMoverSell = sorted.find(e => e.win1Delta < 0) || sorted[sorted.length - 1]

    // Maior pressão
    const highestPressure = [...entries].sort((a, b) => b.pressure - a.pressure)[0]
    const lowestPressure  = [...entries].sort((a, b) => a.pressure - b.pressure)[0]

    return {
      market,
      totalBuyVol,
      totalSellVol,
      totalVol,
      buyRatio,          // % de compra — sessão
      win1Ratio,         // % de compra — último minuto
      impactRatio,       // % de impacto dos compradores no preço
      dominantSide:      buyRatio >= 50 ? 'buy' : 'sell',
      priceMover:        impactRatio >= 50 ? 'buyers' : 'sellers',
      delta:             totalBuyVol - totalSellVol,
      win1Delta:         totalWin1Buy - totalWin1Sell,
      topMoverBuy,
      topMoverSell,
      highestPressure,
      lowestPressure,
      ts: Date.now(),
    }
  }

  // ─────────────────────────────────────────────────────────────
  // START / STOP
  // ─────────────────────────────────────────────────────────────

  /**
   * Inicia a recolha de dados.
   * @param {Object} [options]
   * @param {number}   [options.topN=25]
   * @param {string[]} [options.markets=['spot','futures']]
   * @param {string}   [options.spotQuote='USDC']      quote asset para spot
   * @param {string}   [options.futuresQuote='USDT']   quote asset para futures
   * @param {number}   [options.windowMs=60000]
   * @param {number}   [options.windowMs5=300000]
   * @param {number}   [options.refreshMs=30000]
   * @param {number}   [options.reconnectMs=3000]
   * @param {number}   [options.maxReconnects=0]
   * @param {string[]} [options.excludeKeywords]
   * @param {boolean}  [options.debug=false]
   * @returns {Promise<void>}
   */
  async function start(options = {}) {
    if (_state.running) {
      console.warn('[BinanceWS] já está a correr. Chama stop() primeiro.')
      return
    }

    _state.opts    = { ...DEFAULTS, ...options }
    _state.running = true

    _log('[BinanceWS] a iniciar...', _state.opts)

    // 1. Carregar rankings via REST
    const markets = _state.opts.markets
    try {
      await Promise.all(markets.map(loadRanking))
    } catch (err) {
      console.error('[BinanceWS] erro REST:', err)
      throw err
    }

    // 2. Abrir streams WS
    markets.forEach(_openStreamsForMarket)

    // 3. Emitir update periódico (snapshot completo a cada segundo)
    const updateTimer = setInterval(() => {
      emit('update', _getAllData())
    }, 1000)

    // 4. Refresh periódico do ranking REST
    _state.refreshTimer = setInterval(async () => {
      _log('[REST] a actualizar rankings...')
      try {
        await Promise.all(markets.map(async market => {
          const oldSyms = _state.symbols[market].join()
          await loadRanking(market)
          const newSyms = _state.symbols[market].join()
          if (oldSyms !== newSyms) {
            _log(`[REST] ${market} ranking mudou — a reconectar streams`)
            _openStreamsForMarket(market)
          }
        }))
      } catch (err) {
        _log('[REST] erro no refresh:', err.message)
      }
    }, _state.opts.refreshMs)

    // Guardar timer para cleanup
    _state._updateTimer = updateTimer

    _log('[BinanceWS] iniciado ✓')
  }

  /**
   * Para todos os streams e limpa o estado.
   */
  function stop() {
    _log('[BinanceWS] a parar...')
    _state.running = false

    clearInterval(_state.refreshTimer)
    clearInterval(_state._updateTimer)

    _state.sockets.forEach((conn, key) => {
      _log(`[WS] closing ${key}`)
      conn._closing = true
      try { conn.ws?.close() } catch(_) {}
    })
    _state.sockets.clear()

    _log('[BinanceWS] parado ✓')
    emit('status', { status: 'stopped' })
  }

  /**
   * Para e reinicia do zero (mantém listeners).
   */
  async function restart(options) {
    stop()
    await new Promise(r => setTimeout(r, 500))
    _state.running   = false
    _state.entries   = { spot: new Map(), futures: new Map() }
    _state.symbols   = { spot: [], futures: [] }
    await start(options || _state.opts)
  }

  // ─────────────────────────────────────────────────────────────
  // UTILITÁRIOS
  // ─────────────────────────────────────────────────────────────

  function _log(...args) {
    if (_state.opts?.debug) console.log('[BinanceWS]', ...args)
  }

  /** Formata volume em K/M/B */
  function fmtVol(n) {
    if (n == null || isNaN(n)) return '—'
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
    return n.toFixed(2)
  }

  /** Formata preço com precisão adaptativa */
  function fmtPrice(n) {
    if (!n) return '—'
    if (n >= 10000) return n.toLocaleString('en-US', { maximumFractionDigits: 2 })
    if (n >= 100)   return n.toFixed(3)
    if (n >= 1)     return n.toFixed(4)
    if (n >= 0.01)  return n.toFixed(5)
    return n.toFixed(6)
  }

  /** Retorna pressão formatada como string */
  function fmtPressure(pressure) {
    const sign = pressure >= 0 ? '+' : ''
    return sign + pressure.toFixed(1)
  }

  /** Estado das conexões WS */
  function getConnectionStatus() {
    const status = {}
    _state.sockets.forEach((conn, key) => {
      const rs = conn.ws?.readyState
      status[key] = rs === 0 ? 'CONNECTING'
                  : rs === 1 ? 'OPEN'
                  : rs === 2 ? 'CLOSING'
                  : rs === 3 ? 'CLOSED'
                  : 'UNKNOWN'
    })
    return status
  }

  // ─────────────────────────────────────────────────────────────
  // API PÚBLICA
  // ─────────────────────────────────────────────────────────────
  return {
    // lifecycle
    start,
    stop,
    restart,

    // eventos
    on,
    off,

    // dados
    getMarket,
    getSymbol,
    getMarketSummary,
    getAllData: _getAllData,

    // utilitários
    fmtVol,
    fmtPrice,
    fmtPressure,
    getConnectionStatus,

    // acesso ao estado (só leitura)
    get symbols()  { return { spot: [..._state.symbols.spot],    futures: [..._state.symbols.futures] } },
    get running()  { return _state.running },
    get version()  { return '1.0.0' },
  }

})) // fim UMD
