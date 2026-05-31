/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  script.js — Binance WebSocket Real-Time Price Updater          ║
 * ║  Versão: 1.0.0                                                  ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  Subscreve streams da Binance e actualiza preços em tempo real  ║
 * ║  Compatível com: Browser (ES Module + UMD) e Node.js            ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  STREAMS SUPORTADOS                                             ║
 * ║  ─────────────────────────────────────────────────────────────  ║
 * ║  miniTicker   — preço, variação 24h, volume (menor overhead)    ║
 * ║  bookTicker   — melhor bid/ask em tempo real                    ║
 * ║  aggTrade     — cada trade individual (buy/sell taker)          ║
 * ║  ticker       — ticker completo 24h                             ║
 * ║  kline        — velas OHLCV por timeframe                       ║
 * ║  depth        — order book parcial (5/10/20 níveis)            ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  USO BÁSICO (Browser)                                           ║
 * ║  ─────────────────────────────────────────────────────────────  ║
 * ║  <script src="script.js"></script>                              ║
 * ║  <script>                                                       ║
 * ║    const tracker = BinancePrice.create()                        ║
 * ║    await tracker.start(['BTCUSDT', 'ETHUSDT'])                  ║
 * ║    tracker.on('price', ({ symbol, price, change }) => {         ║
 * ║      document.getElementById('btc').textContent = price         ║
 * ║    })                                                           ║
 * ║  </script>                                                      ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  USO BÁSICO (Node.js)                                           ║
 * ║  ─────────────────────────────────────────────────────────────  ║
 * ║  npm install ws node-fetch                                      ║
 * ║  const { create } = require('./script')                         ║
 * ║  const tracker = create()                                       ║
 * ║  await tracker.start(['BTCUSDT', 'ETHUSDT'])                    ║
 * ║  tracker.on('price', d => console.log(d.symbol, d.price))       ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║  EVENTOS                                                        ║
 * ║  ─────────────────────────────────────────────────────────────  ║
 * ║  'price'      — preço actualizado (miniTicker + bookTicker)     ║
 * ║  'trade'      — trade individual (aggTrade)                     ║
 * ║  'ticker'     — ticker completo 24h                             ║
 * ║  'kline'      — vela actualizada                                ║
 * ║  'depth'      — order book actualizado                          ║
 * ║  'connect'    — stream conectado                                ║
 * ║  'disconnect' — stream desconectado                             ║
 * ║  'error'      — erro de conexão                                 ║
 * ║  'status'     — mudança de estado geral                         ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */

;(function (root, factory) {
  /* UMD — funciona em Node.js (CommonJS), browser (global) e bundlers (AMD) */
  if (typeof module !== 'undefined' && module.exports) {
    /* Node.js */
    const WSImpl = (function () {
      if (typeof WebSocket !== 'undefined') return WebSocket
      try { return require('ws') }
      catch (_) { throw new Error('[BinancePrice] Node.js: npm install ws') }
    })()
    const fetchImpl = (function () {
      if (typeof fetch !== 'undefined') return fetch.bind(globalThis)
      try { return require('node-fetch') }
      catch (_) { throw new Error('[BinancePrice] Node.js: npm install node-fetch') }
    })()
    module.exports = factory(WSImpl, fetchImpl)
  } else if (typeof define === 'function' && define.amd) {
    /* AMD */
    define([], () => factory(WebSocket, fetch.bind(window)))
  } else {
    /* Browser global */
    root.BinancePrice = factory(WebSocket, fetch.bind(window))
  }
}(typeof globalThis !== 'undefined' ? globalThis : this, function (WSImpl, fetchFn) {
  'use strict'

  /* ════════════════════════════════════════════════════════════════
     ENDPOINTS
  ════════════════════════════════════════════════════════════════ */

  const ENDPOINTS = {
    spot: {
      rest: 'https://api.binance.com/api/v3',
      ws:   'wss://stream.binance.com:9443/stream',
    },
    futures: {
      rest: 'https://fapi.binance.com/fapi/v1',
      ws:   'wss://fstream.binance.com/stream',
    },
    coin: {
      rest: 'https://dapi.binance.com/dapi/v1',
      ws:   'wss://dstream.binance.com/stream',
    },
  }

  /* ════════════════════════════════════════════════════════════════
     DEFAULTS
  ════════════════════════════════════════════════════════════════ */

  const DEFAULTS = {
    /** Mercado: 'spot' | 'futures' | 'coin' */
    market: 'spot',

    /**
     * Tipo de stream principal para preços.
     * 'mini'  → miniTicker  (menor overhead, recomendado)
     * 'book'  → bookTicker  (bid/ask em tempo real)
     * 'full'  → ticker 24h completo
     * 'both'  → miniTicker + bookTicker em simultâneo
     */
    priceStream: 'mini',

    /** Número máximo de streams por conexão WebSocket (limite Binance: 1024) */
    maxStreamsPerConn: 300,

    /** Reconectar automaticamente após perda de conexão */
    autoReconnect: true,

    /** Atraso base de reconexão em ms (aumenta com backoff exponencial) */
    reconnectDelay: 3_000,

    /** Máximo de tentativas de reconexão (0 = ilimitado) */
    maxReconnects: 0,

    /** Intervalo de ping manual ao servidor WS em ms (< 3 min para evitar timeout) */
    pingInterval: 150_000,

    /** Activar logs de debug */
    debug: false,
  }

  /* ════════════════════════════════════════════════════════════════
     CRIA INSTÂNCIA
  ════════════════════════════════════════════════════════════════ */

  /**
   * Cria uma nova instância do tracker de preços.
   *
   * @param {object} [opts] — opções (ver DEFAULTS acima)
   * @returns {PriceTracker}
   */
  function create (opts) {
    return new PriceTracker(opts)
  }

  /* ════════════════════════════════════════════════════════════════
     PRICE TRACKER CLASS
  ════════════════════════════════════════════════════════════════ */

  /**
   * @typedef {object} PriceData
   * Dados emitidos no evento 'price':
   * @property {string}  symbol       — símbolo (ex: 'BTCUSDT')
   * @property {number}  price        — preço actual
   * @property {number}  prevPrice    — preço anterior
   * @property {number}  open         — abertura 24h
   * @property {number}  high         — máximo 24h
   * @property {number}  low          — mínimo 24h
   * @property {number}  change       — variação absoluta 24h
   * @property {number}  changePct    — variação % 24h
   * @property {number}  volume       — volume 24h (base asset)
   * @property {number}  quoteVolume  — volume 24h (quote asset)
   * @property {number}  bidPrice     — melhor bid (se stream 'book' activo)
   * @property {number}  bidQty       — quantidade bid
   * @property {number}  askPrice     — melhor ask
   * @property {number}  askQty       — quantidade ask
   * @property {number}  spread       — ask − bid
   * @property {number}  spreadPct    — spread em %
   * @property {number}  lastTs       — timestamp em ms
   * @property {string}  market       — 'spot' | 'futures' | 'coin'
   */

  /**
   * @typedef {object} TradeData
   * Dados emitidos no evento 'trade':
   * @property {string}  symbol       — símbolo
   * @property {number}  price        — preço do trade
   * @property {number}  qty          — quantidade
   * @property {number}  quoteQty     — quantidade em quote (price * qty)
   * @property {boolean} isBuy        — true = comprador foi agressivo (taker buy)
   * @property {number}  ts           — timestamp ms
   * @property {string}  tradeId      — ID do trade agregado
   */

  /**
   * @typedef {object} KlineData
   * Dados emitidos no evento 'kline':
   * @property {string}  symbol
   * @property {string}  interval     — '1m' | '5m' | '1h' | etc.
   * @property {number}  open
   * @property {number}  high
   * @property {number}  low
   * @property {number}  close
   * @property {number}  volume
   * @property {boolean} isClosed     — true = vela fechada
   * @property {number}  openTime     — timestamp ms
   * @property {number}  closeTime    — timestamp ms
   */

  function PriceTracker (opts) {
    this._opts    = Object.assign({}, DEFAULTS, opts || {})
    this._state   = new Map()      // symbol → PriceData
    this._conns   = []             // array de WSConn
    this._subs    = new Map()      // symbol → Set<'mini'|'book'|'trade'|'kline'|'depth'>
    this._klSubs  = new Map()      // symbol+interval → Set
    this._dpSubs  = new Map()      // symbol+depth → Set
    this._listeners = new Map()    // event → Set<fn>
    this._running = false
    this._pingTimers = []
  }

  /* ─── CYCLE ─────────────────────────────────────────────────── */

  /**
   * Inicia o tracker e subscreve streams de preço para os símbolos fornecidos.
   *
   * @param {string[]} symbols — lista de símbolos, ex: ['BTCUSDT','ETHUSDT']
   * @param {object}   [startOpts]
   * @param {boolean}  [startOpts.prefetch=true]  — carregar preços iniciais via REST antes do WS
   * @returns {Promise<void>}
   */
  PriceTracker.prototype.start = async function (symbols, startOpts) {
    if (this._running) {
      console.warn('[BinancePrice] já em execução. Chama stop() primeiro.')
      return
    }
    this._running = true
    const opts = Object.assign({ prefetch: true }, startOpts || {})

    this._log('start()', symbols.length, 'símbolos')

    /* Inicializar state para cada símbolo */
    symbols.forEach(sym => {
      if (!this._state.has(sym)) this._state.set(sym, _mkState(sym, this._opts.market))
      const subs = this._subs.get(sym) || new Set()
      this._subs.set(sym, subs)
    })

    /* Carregar preços iniciais via REST (evita "aguardar primeiro tick WS") */
    if (opts.prefetch) {
      await this._fetchInitial(symbols)
    }

    /* Abrir streams de preço */
    this._openPriceStreams(symbols)

    this._emit('status', { status: 'started', symbols, market: this._opts.market })
  }

  /**
   * Para todos os streams e liberta recursos.
   */
  PriceTracker.prototype.stop = function () {
    this._running = false
    this._pingTimers.forEach(t => clearInterval(t))
    this._pingTimers = []
    this._conns.forEach(c => {
      c.closing = true
      try { c.ws.close() } catch (_) {}
    })
    this._conns = []
    this._emit('status', { status: 'stopped' })
    this._log('stop()')
  }

  /**
   * Para e reinicia com novos símbolos (mantém listeners).
   * @param {string[]} symbols
   */
  PriceTracker.prototype.restart = async function (symbols) {
    this.stop()
    this._running = false
    this._state   = new Map()
    this._subs    = new Map()
    this._klSubs  = new Map()
    this._dpSubs  = new Map()
    await new Promise(r => setTimeout(r, 500))
    await this.start(symbols)
  }

  /* ─── ADICIONAR/REMOVER SÍMBOLOS EM RUNTIME ─────────────────── */

  /**
   * Adiciona símbolos ao tracker enquanto está a correr.
   * @param {string[]} symbols
   */
  PriceTracker.prototype.subscribe = async function (symbols) {
    const newSyms = symbols.filter(s => !this._subs.has(s))
    if (!newSyms.length) return

    newSyms.forEach(sym => {
      if (!this._state.has(sym)) this._state.set(sym, _mkState(sym, this._opts.market))
      this._subs.set(sym, new Set())
    })

    await this._fetchInitial(newSyms)
    this._openPriceStreams(newSyms)
    this._log('subscribe()', newSyms)
  }

  /**
   * Remove símbolos do tracker.
   * Fecha as conexões WS dos streams removidos (se exclusivas).
   * @param {string[]} symbols
   */
  PriceTracker.prototype.unsubscribe = function (symbols) {
    symbols.forEach(sym => {
      this._subs.delete(sym)
      this._state.delete(sym)
    })
    // Reconectar streams (elimina os streams dos símbolos removidos)
    const remaining = Array.from(this._subs.keys())
    this._closeAllConns()
    if (remaining.length) this._openPriceStreams(remaining)
    this._log('unsubscribe()', symbols)
  }

  /* ─── STREAMS ADICIONAIS ─────────────────────────────────────── */

  /**
   * Subscreve stream de trades individuais (aggTrade) para um ou mais símbolos.
   * Emite evento 'trade' com dados de cada trade.
   * @param {string|string[]} symbols
   */
  PriceTracker.prototype.subscribeTrades = function (symbols) {
    const syms = Array.isArray(symbols) ? symbols : [symbols]
    const streams = syms.map(s => s.toLowerCase() + '@aggTrade')
    this._openRawStreams(streams, 'trade')
    this._log('subscribeTrades()', syms)
  }

  /**
   * Subscreve stream de velas (kline) para um símbolo e intervalo.
   * Emite evento 'kline' a cada actualização.
   *
   * @param {string} symbol      — ex: 'BTCUSDT'
   * @param {string} interval    — '1m'|'3m'|'5m'|'15m'|'30m'|'1h'|'2h'|'4h'|'6h'|'12h'|'1d'|'3d'|'1w'|'1M'
   */
  PriceTracker.prototype.subscribeKline = function (symbol, interval) {
    const stream = symbol.toLowerCase() + '@kline_' + interval
    this._openRawStreams([stream], 'kline')
    this._log('subscribeKline()', symbol, interval)
  }

  /**
   * Subscreve stream de order book parcial.
   * Emite evento 'depth' com bids e asks.
   *
   * @param {string} symbol   — ex: 'BTCUSDT'
   * @param {number} [levels] — 5 | 10 | 20
   * @param {number} [speed]  — 100 (100ms) | 1000 (1s)
   */
  PriceTracker.prototype.subscribeDepth = function (symbol, levels, speed) {
    const lvl   = [5, 10, 20].includes(levels) ? levels : 10
    const spd   = speed === 100 ? '@100ms' : ''
    const stream = symbol.toLowerCase() + '@depth' + lvl + spd
    this._openRawStreams([stream], 'depth')
    this._log('subscribeDepth()', symbol, lvl)
  }

  /**
   * Subscreve ticker 24h completo para símbolos adicionais.
   * Emite evento 'ticker'.
   * @param {string[]} symbols
   */
  PriceTracker.prototype.subscribeTicker = function (symbols) {
    const syms = Array.isArray(symbols) ? symbols : [symbols]
    const streams = syms.map(s => s.toLowerCase() + '@ticker')
    this._openRawStreams(streams, 'ticker')
    this._log('subscribeTicker()', syms)
  }

  /* ─── EVENTOS ─────────────────────────────────────────────────── */

  /**
   * Regista um listener para um evento.
   *
   * @param {string}   event  — 'price'|'trade'|'kline'|'depth'|'ticker'|'connect'|'disconnect'|'error'|'status'
   * @param {function} fn     — callback(data)
   * @returns {function}        função para remover o listener (unsubscribe)
   *
   * @example
   * const off = tracker.on('price', data => {
   *   console.log(data.symbol, data.price, data.changePct + '%')
   * })
   * // Para remover:
   * off()
   */
  PriceTracker.prototype.on = function (event, fn) {
    if (!this._listeners.has(event)) this._listeners.set(event, new Set())
    this._listeners.get(event).add(fn)
    return () => this.off(event, fn)
  }

  /**
   * Remove um listener.
   * @param {string}   event
   * @param {function} fn
   */
  PriceTracker.prototype.off = function (event, fn) {
    this._listeners.get(event)?.delete(fn)
  }

  /**
   * Regista um listener que dispara apenas uma vez.
   * @param {string}   event
   * @param {function} fn
   */
  PriceTracker.prototype.once = function (event, fn) {
    const off = this.on(event, data => { fn(data); off() })
  }

  /* ─── DADOS ─────────────────────────────────────────────────── */

  /**
   * Obtém os dados de preço actuais de um símbolo.
   * @param {string} symbol
   * @returns {PriceData|null}
   */
  PriceTracker.prototype.getPrice = function (symbol) {
    const s = this._state.get(symbol)
    return s ? _snapState(s) : null
  }

  /**
   * Obtém todos os dados de preço actuais.
   * @returns {PriceData[]}
   */
  PriceTracker.prototype.getAllPrices = function () {
    return Array.from(this._state.values()).map(_snapState)
  }

  /**
   * Obtém o estado das conexões WebSocket.
   * @returns {object[]}
   */
  PriceTracker.prototype.getConnectionStatus = function () {
    return this._conns.map((c, i) => ({
      idx:        i,
      streams:    c.streams.length,
      state:      ['CONNECTING','OPEN','CLOSING','CLOSED'][c.ws?.readyState] || 'UNKNOWN',
      reconnects: c.reconnects,
    }))
  }

  /* ─── INTERNO — REST ─────────────────────────────────────────── */

  PriceTracker.prototype._fetchInitial = async function (symbols) {
    const market  = this._opts.market
    const baseUrl = ENDPOINTS[market].rest
    try {
      /* Binance suporta single-ticker e multi-ticker */
      const url = symbols.length === 1
        ? `${baseUrl}/ticker/24hr?symbol=${symbols[0]}`
        : `${baseUrl}/ticker/24hr`
      const res  = await fetchFn(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      const arr  = Array.isArray(data) ? data : [data]
      const symSet = new Set(symbols)
      arr.filter(t => symSet.has(t.symbol)).forEach(t => {
        const s = this._state.get(t.symbol)
        if (!s) return
        _applyTicker24h(s, t)
        this._emit('price', _snapState(s))
      })
      this._log('fetchInitial()', symbols.length, 'tickers carregados')
    } catch (err) {
      this._log('fetchInitial() erro:', err.message)
    }
  }

  /* ─── INTERNO — STREAMS ─────────────────────────────────────── */

  PriceTracker.prototype._openPriceStreams = function (symbols) {
    const mode   = this._opts.priceStream
    const market = this._opts.market
    const streams = []

    symbols.forEach(sym => {
      const sl = sym.toLowerCase()
      if (mode === 'mini' || mode === 'both') streams.push(sl + '@miniTicker')
      if (mode === 'book' || mode === 'both') streams.push(sl + '@bookTicker')
      if (mode === 'full')                    streams.push(sl + '@ticker')
    })

    this._openRawStreams(streams, 'price')
  }

  PriceTracker.prototype._openRawStreams = function (streamNames, type) {
    const market  = this._opts.market
    const base    = ENDPOINTS[market].ws
    const maxPer  = this._opts.maxStreamsPerConn

    /* Dividir em grupos respeitando o limite por conexão */
    for (let i = 0; i < streamNames.length; i += maxPer) {
      const group = streamNames.slice(i, i + maxPer)
      const url   = base + '?streams=' + group.join('/')
      const conn  = { ws: null, streams: group, type, closing: false, reconnects: 0 }
      this._conns.push(conn)
      this._connectWS(conn, url)
    }
  }

  PriceTracker.prototype._connectWS = function (conn, url) {
    this._log(`[WS] conectando (${conn.streams.length} streams, tipo:${conn.type})`)

    const ws = new WSImpl(url)
    conn.ws  = ws

    ws.onopen = () => {
      this._log('[WS] aberto')
      conn.reconnects = 0

      /* Ping periódico para manter conexão viva */
      const pingTimer = setInterval(() => {
        if (ws.readyState === 1) {
          try {
            if (typeof ws.ping === 'function') ws.ping()
            /* Em browser não existe ws.ping() — o servidor envia ping automaticamente */
          } catch (_) {}
        }
      }, this._opts.pingInterval)
      this._pingTimers.push(pingTimer)

      this._emit('connect', { streams: conn.streams, type: conn.type })
    }

    ws.onmessage = ev => {
      try {
        const raw = typeof ev.data === 'string' ? ev.data : ev.data.toString()
        const msg = JSON.parse(raw)
        /* Combined stream format: { stream: "name@type", data: {...} } */
        const d = msg.data || msg
        if (!d) return
        this._routeMessage(d, conn.type)
      } catch (_) {}
    }

    ws.onerror = err => {
      this._log('[WS] erro:', err?.message || 'desconhecido')
      this._emit('error', { error: err, streams: conn.streams })
    }

    ws.onclose = () => {
      this._log('[WS] fechado')
      this._emit('disconnect', { streams: conn.streams })
      if (!conn.closing && this._opts.autoReconnect && this._running) {
        const maxR = this._opts.maxReconnects
        if (maxR > 0 && conn.reconnects >= maxR) {
          this._log('[WS] máximo de reconexões atingido')
          return
        }
        /* Backoff exponencial (máx 30s) */
        const delay = Math.min(this._opts.reconnectDelay * Math.pow(1.5, conn.reconnects), 30_000)
        conn.reconnects++
        this._log(`[WS] reconexão #${conn.reconnects} em ${Math.round(delay)}ms`)
        setTimeout(() => this._connectWS(conn, url), delay)
      }
    }
  }

  PriceTracker.prototype._closeAllConns = function () {
    this._conns.forEach(c => { c.closing = true; try { c.ws.close() } catch (_) {} })
    this._conns = []
  }

  /* ─── INTERNO — ROTEAMENTO DE MENSAGENS ─────────────────────── */

  PriceTracker.prototype._routeMessage = function (d, type) {
    const e = d.e  /* tipo de evento Binance */

    switch (e) {
      /* ── miniTicker ───────────────────────────────────────── */
      case '24hrMiniTicker': {
        const s = this._state.get(d.s)
        if (!s) return
        const prev = s.price
        s.prevPrice    = prev
        s.price        = +d.c
        s.open         = +d.o
        s.high         = +d.h
        s.low          = +d.l
        s.volume       = +d.v
        s.quoteVolume  = +d.q
        s.change       = +d.c - +d.o
        s.changePct    = +d.o > 0 ? ((+d.c - +d.o) / +d.o) * 100 : 0
        s.lastTs       = d.E
        this._emit('price', _snapState(s))
        break
      }

      /* ── bookTicker ──────────────────────────────────────── */
      case undefined: /* bookTicker não tem campo 'e' */
      case 'bookTicker': {
        /* bookTicker: { u, s, b, B, a, A } */
        if (!d.s || !d.b) break
        const s = this._state.get(d.s)
        if (!s) return
        s.bidPrice  = +d.b
        s.bidQty    = +d.B
        s.askPrice  = +d.a
        s.askQty    = +d.A
        s.spread    = +d.a - +d.b
        s.spreadPct = +d.b > 0 ? (+d.a - +d.b) / +d.b * 100 : 0
        /* Actualizar preço com mid-price se não tiver miniTicker */
        if (!s.price) s.price = (+d.b + +d.a) / 2
        this._emit('price', _snapState(s))
        break
      }

      /* ── ticker 24h completo ─────────────────────────────── */
      case '24hrTicker': {
        const s = this._state.get(d.s)
        if (!s) return
        s.prevPrice   = s.price
        s.price       = +d.c
        s.open        = +d.o
        s.high        = +d.h
        s.low         = +d.l
        s.volume      = +d.v
        s.quoteVolume = +d.q
        s.change      = +d.p
        s.changePct   = +d.P
        s.bidPrice    = +d.b
        s.askPrice    = +d.a
        s.spread      = +d.a - +d.b
        s.spreadPct   = +d.b > 0 ? (+d.a - +d.b) / +d.b * 100 : 0
        s.lastTs      = d.E
        this._emit('price', _snapState(s))
        this._emit('ticker', {
          symbol:      d.s,
          price:       +d.c,
          change:      +d.p,
          changePct:   +d.P,
          high:        +d.h,
          low:         +d.l,
          volume:      +d.v,
          quoteVolume: +d.q,
          open:        +d.o,
          bidPrice:    +d.b,
          askPrice:    +d.a,
          trades:      +d.n,
          ts:          d.E,
        })
        break
      }

      /* ── aggTrade ────────────────────────────────────────── */
      case 'aggTrade': {
        const s = this._state.get(d.s)
        if (s) {
          s.prevPrice = s.price
          s.price     = +d.p
          s.lastTs    = d.T
        }
        /** @type {TradeData} */
        const trade = {
          symbol:   d.s,
          price:    +d.p,
          qty:      +d.q,
          quoteQty: +d.p * +d.q,
          isBuy:    !d.m,   /* m=false → buyer was taker = BUY */
          ts:       d.T,
          tradeId:  String(d.a),
          market:   this._opts.market,
        }
        if (s) this._emit('price', _snapState(s))
        this._emit('trade', trade)
        break
      }

      /* ── kline ───────────────────────────────────────────── */
      case 'kline': {
        const k = d.k
        if (!k) break
        const s = this._state.get(d.s)
        if (s) {
          s.prevPrice = s.price
          s.price     = +k.c
          s.lastTs    = k.t
        }
        /** @type {KlineData} */
        const kline = {
          symbol:    d.s,
          interval:  k.i,
          open:      +k.o,
          high:      +k.h,
          low:       +k.l,
          close:     +k.c,
          volume:    +k.v,
          quoteVol:  +k.q,
          trades:    +k.n,
          isClosed:  k.x,
          openTime:  k.t,
          closeTime: k.T,
          market:    this._opts.market,
        }
        if (s) this._emit('price', _snapState(s))
        this._emit('kline', kline)
        break
      }

      /* ── depth (order book parcial) ─────────────────────── */
      case 'depthUpdate': {
        this._emit('depth', {
          symbol:        d.s,
          lastUpdateId:  d.u,
          bids:          (d.b || []).map(r => ({ price: +r[0], qty: +r[1] })),
          asks:          (d.a || []).map(r => ({ price: +r[0], qty: +r[1] })),
          ts:            d.E,
          market:        this._opts.market,
        })
        break
      }

      /* ── markPriceUpdate (futures) ───────────────────────── */
      case 'markPriceUpdate': {
        const s = this._state.get(d.s)
        if (!s) return
        s.markPrice    = +d.p
        s.indexPrice   = +d.i
        s.fundingRate  = +d.r
        s.nextFunding  = +d.T
        s.lastTs       = d.E
        this._emit('price', _snapState(s))
        break
      }

      default:
        /* Mensagem desconhecida — ignorar */
        break
    }
  }

  /* ─── INTERNO — HELPERS ─────────────────────────────────────── */

  PriceTracker.prototype._emit = function (event, data) {
    const ls = this._listeners.get(event)
    if (!ls || !ls.size) return
    ls.forEach(fn => {
      try { fn(data) }
      catch (err) { console.error('[BinancePrice] listener error:', err) }
    })
  }

  PriceTracker.prototype._log = function (...args) {
    if (this._opts.debug) console.log('[BinancePrice]', ...args)
  }

  /* ════════════════════════════════════════════════════════════════
     STATE FACTORY & HELPERS
  ════════════════════════════════════════════════════════════════ */

  function _mkState (symbol, market) {
    return {
      symbol,
      market,
      price:       0,
      prevPrice:   0,
      open:        0,
      high:        0,
      low:         0,
      change:      0,
      changePct:   0,
      volume:      0,
      quoteVolume: 0,
      bidPrice:    0,
      bidQty:      0,
      askPrice:    0,
      askQty:      0,
      spread:      0,
      spreadPct:   0,
      markPrice:   0,
      indexPrice:  0,
      fundingRate: 0,
      nextFunding: 0,
      lastTs:      0,
    }
  }

  function _snapState (s) {
    return Object.assign({}, s)
  }

  function _applyTicker24h (s, t) {
    s.price       = +t.lastPrice
    s.prevPrice   = +t.lastPrice
    s.open        = +t.openPrice
    s.high        = +t.highPrice
    s.low         = +t.lowPrice
    s.change      = +t.priceChange
    s.changePct   = +t.priceChangePercent
    s.volume      = +t.volume
    s.quoteVolume = +t.quoteVolume
    s.bidPrice    = +t.bidPrice  || 0
    s.askPrice    = +t.askPrice  || 0
    s.lastTs      = +t.closeTime || Date.now()
    if (s.askPrice && s.bidPrice) {
      s.spread    = s.askPrice - s.bidPrice
      s.spreadPct = s.bidPrice > 0 ? s.spread / s.bidPrice * 100 : 0
    }
  }

  /* ════════════════════════════════════════════════════════════════
     FORMATOS UTILITÁRIOS (exportados)
  ════════════════════════════════════════════════════════════════ */

  /**
   * Formata preço com precisão adaptativa.
   * @param {number} n
   * @returns {string}
   */
  function formatPrice (n) {
    if (n == null || isNaN(n)) return '—'
    if (n >= 10000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    if (n >= 100)   return n.toFixed(3)
    if (n >= 1)     return n.toFixed(4)
    if (n >= 0.01)  return n.toFixed(5)
    return n.toFixed(6)
  }

  /**
   * Formata volume em K/M/B.
   * @param {number} n
   * @returns {string}
   */
  function formatVolume (n) {
    if (n == null || isNaN(n)) return '—'
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
    return n.toFixed(2)
  }

  /**
   * Formata variação percentual.
   * @param {number} n
   * @param {number} [decimals=2]
   * @returns {string}
   */
  function formatChange (n, decimals) {
    if (n == null || isNaN(n)) return '—'
    const d = decimals ?? 2
    return (n >= 0 ? '+' : '') + n.toFixed(d) + '%'
  }

  /**
   * Retorna a cor CSS para um valor (positivo = verde, negativo = vermelho).
   * @param {number}  n
   * @param {string} [positiveColor='#00d48a']
   * @param {string} [negativeColor='#ff3860']
   * @returns {string}
   */
  function priceColor (n, positiveColor, negativeColor) {
    const pos = positiveColor || '#00d48a'
    const neg = negativeColor || '#ff3860'
    if (n == null || isNaN(n)) return '#888'
    return n > 0 ? pos : n < 0 ? neg : '#888'
  }

  /* ════════════════════════════════════════════════════════════════
     DOM HELPER — actualiza elementos HTML automaticamente
  ════════════════════════════════════════════════════════════════ */

  /**
   * Utilitário de binding DOM.
   * Liga o tracker a elementos HTML via atributos data-.
   *
   * Atributos suportados nos elementos:
   *   data-bp-symbol="BTCUSDT"      — símbolo a monitorizar
   *   data-bp-field="price"         — campo a exibir (price|changePct|change|high|low|volume|bidPrice|askPrice|spread)
   *   data-bp-format="price"        — formato (price|volume|change|raw)
   *   data-bp-color="true"          — aplica cor CSS ao elemento
   *   data-bp-color-field="changePct" — campo usado para determinar a cor
   *   data-bp-flash="true"          — aplica flash de cor ao actualizar
   *
   * @param {PriceTracker} tracker
   * @param {Element}      [container=document]
   * @returns {function}  função para desligar o binding
   *
   * @example
   * <!-- HTML -->
   * <span data-bp-symbol="BTCUSDT" data-bp-field="price" data-bp-format="price" data-bp-color="true" data-bp-color-field="changePct" data-bp-flash="true">—</span>
   * <span data-bp-symbol="BTCUSDT" data-bp-field="changePct" data-bp-format="change" data-bp-color="true"></span>
   *
   * <!-- JS -->
   * bindDOM(tracker)
   */
  function bindDOM (tracker, container) {
    const root = container || (typeof document !== 'undefined' ? document : null)
    if (!root) return () => {}

    const formatters = {
      price:  formatPrice,
      volume: formatVolume,
      change: formatChange,
      raw:    v => v == null ? '—' : String(v),
    }

    const off = tracker.on('price', data => {
      const els = root.querySelectorAll(`[data-bp-symbol="${data.symbol}"]`)
      els.forEach(el => {
        const field    = el.getAttribute('data-bp-field')    || 'price'
        const fmt      = el.getAttribute('data-bp-format')   || 'raw'
        const applyClr = el.getAttribute('data-bp-color')    === 'true'
        const clrField = el.getAttribute('data-bp-color-field') || field
        const flash    = el.getAttribute('data-bp-flash')    === 'true'
        const prevVal  = el._bpPrev

        const rawVal  = data[field]
        const clrVal  = data[clrField]
        const display = (formatters[fmt] || formatters.raw)(rawVal)

        el.textContent = display

        /* Cor dinâmica */
        if (applyClr) {
          el.style.color = priceColor(clrVal)
        }

        /* Flash ao mudar */
        if (flash && prevVal !== undefined && rawVal !== prevVal) {
          const dir = rawVal > prevVal ? 'up' : 'dn'
          el.classList.remove('bp-flash-up', 'bp-flash-dn')
          void el.offsetWidth /* reflow */
          el.classList.add('bp-flash-' + dir)
          setTimeout(() => el.classList.remove('bp-flash-up', 'bp-flash-dn'), 700)
        }

        el._bpPrev = rawVal
      })
    })

    /* Injectar CSS de flash se não existir */
    if (typeof document !== 'undefined' && !document.getElementById('bp-flash-css')) {
      const style = document.createElement('style')
      style.id    = 'bp-flash-css'
      style.textContent = `
        @keyframes bp-flash-up-anim { 0% { background: rgba(0,212,138,.35); } 100% { background: transparent; } }
        @keyframes bp-flash-dn-anim { 0% { background: rgba(255,56,96,.35); } 100% { background: transparent; } }
        .bp-flash-up { animation: bp-flash-up-anim .65s ease-out; }
        .bp-flash-dn { animation: bp-flash-dn-anim .65s ease-out; }
      `
      document.head.appendChild(style)
    }

    return off
  }

  /* ════════════════════════════════════════════════════════════════
     API PÚBLICA
  ════════════════════════════════════════════════════════════════ */

  return {
    /* Criação de instância */
    create,

    /* Formatadores */
    formatPrice,
    formatVolume,
    formatChange,
    priceColor,

    /* DOM binding */
    bindDOM,

    /* Constantes */
    ENDPOINTS,
    VERSION: '1.0.0',
  }
}))

/* ════════════════════════════════════════════════════════════════════
   EXEMPLOS DE USO
   ─────────────────────────────────────────────────────────────────

   ──── 1. Básico — actualizar preço de BTC ────────────────────────
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT'])
   tracker.on('price', data => {
     document.getElementById('price').textContent = BinancePrice.formatPrice(data.price)
   })

   ──── 2. Com DOM binding automático ──────────────────────────────
   <!-- HTML -->
   <span data-bp-symbol="BTCUSDT" data-bp-field="price"
         data-bp-format="price" data-bp-color="true"
         data-bp-color-field="changePct" data-bp-flash="true">—</span>
   <span data-bp-symbol="BTCUSDT" data-bp-field="changePct"
         data-bp-format="change" data-bp-color="true">—</span>
   <!-- JS -->
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
   BinancePrice.bindDOM(tracker)  // auto-actualiza todos os elementos data-bp-*

   ──── 3. Múltiplos mercados ───────────────────────────────────────
   const spotTracker = BinancePrice.create({ market: 'spot' })
   const futTracker  = BinancePrice.create({ market: 'futures' })
   await spotTracker.start(['BTCUSDT', 'ETHUSDT'])
   await futTracker.start(['BTCUSDT', 'ETHUSDT'])
   spotTracker.on('price', data => console.log('SPOT', data.symbol, data.price))
   futTracker.on('price', data => console.log('FUTURES', data.symbol, data.price, 'FR:', data.fundingRate))

   ──── 4. Bid/Ask em tempo real ────────────────────────────────────
   const tracker = BinancePrice.create({ priceStream: 'book' })
   await tracker.start(['BTCUSDT'])
   tracker.on('price', ({ symbol, bidPrice, askPrice, spread, spreadPct }) => {
     console.log(`${symbol} BID: ${bidPrice} ASK: ${askPrice} SPREAD: ${spreadPct.toFixed(4)}%`)
   })

   ──── 5. Trades individuais (buy/sell) ───────────────────────────
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT'])
   tracker.subscribeTrades(['BTCUSDT', 'ETHUSDT'])
   tracker.on('trade', ({ symbol, price, qty, isBuy }) => {
     console.log(symbol, isBuy ? 'BUY' : 'SELL', price, qty)
   })

   ──── 6. Velas em tempo real ──────────────────────────────────────
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT'])
   tracker.subscribeKline('BTCUSDT', '1h')
   tracker.on('kline', ({ symbol, interval, open, high, low, close, isClosed }) => {
     if (isClosed) console.log('Vela fechada:', symbol, interval, close)
   })

   ──── 7. Order book ───────────────────────────────────────────────
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT'])
   tracker.subscribeDepth('BTCUSDT', 10, 100)  // 10 níveis, 100ms
   tracker.on('depth', ({ symbol, bids, asks }) => {
     console.log('Melhor bid:', bids[0], 'Melhor ask:', asks[0])
   })

   ──── 8. Adicionar/remover símbolos em runtime ───────────────────
   const tracker = BinancePrice.create()
   await tracker.start(['BTCUSDT'])
   await tracker.subscribe(['ETHUSDT', 'SOLUSDT'])  // adicionar
   tracker.unsubscribe(['BTCUSDT'])                 // remover

   ──── 9. Obter snapshot actual ────────────────────────────────────
   const data = tracker.getPrice('BTCUSDT')
   console.log(data.price, data.changePct, data.volume)
   const all = tracker.getAllPrices()
   all.sort((a,b) => b.changePct - a.changePct).slice(0,5)  // top gainers

   ──── 10. Futures — mark price + funding rate ────────────────────
   const tracker = BinancePrice.create({ market: 'futures', priceStream: 'mini' })
   await tracker.start(['BTCUSDT', 'ETHUSDT'])
   tracker.on('price', data => {
     if (data.markPrice) {
       console.log(data.symbol, 'Mark:', data.markPrice, 'FR:', (data.fundingRate*100).toFixed(4)+'%')
     }
   })

════════════════════════════════════════════════════════════════════ */