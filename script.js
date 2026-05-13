const spotSymbols = ['BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'BNBUSDC', 'XRPUSDC', 'ADAUSDC']; // Top USDC Spot
const futSymbols = ['BTCUSDC', 'ETHUSDC', 'SOLUSDC', 'XRPUSDC', 'DOGEUSDC']; // Top USDC Perps

const state = { spot: {}, futures: {}, alerts: [] };

// Iniciar WebSockets
const wsSpot = new WebSocket('wss://stream.binance.com:9443/ws');
const wsFut = new WebSocket('wss://fstream.binance.com/ws');

function initWS(ws, symbols, type) {
    ws.onopen = () => {
        const msg = { method: "SUBSCRIBE", params: symbols.map(s => `${s.toLowerCase()}@aggTrade`), id: type === 'spot' ? 1 : 2 };
        ws.send(JSON.stringify(msg));
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.e === 'aggTrade') updateData(data, type);
    };
}

initWS(wsSpot, spotSymbols, 'spot');
initWS(wsFut, futSymbols, 'futures');

function updateData(trade, type) {
    const symbol = trade.s;
    const price = parseFloat(trade.p);
    const qty = parseFloat(trade.q);
    const isSellerAggressive = trade.m; 

    if (!state[type][symbol]) state[type][symbol] = { buyVol: 0, sellVol: 0, price: 0 };
    
    const coin = state[type][symbol];
    coin.price = price;
    isSellerAggressive ? (coin.sellVol += qty) : (coin.buyVol += qty);

    renderRow(symbol, type);
    checkPriceAlerts(symbol, price);
}

function renderRow(symbol, type) {
    const data = state[type][symbol];
    const tbody = document.getElementById(`${type}Body`);
    let row = document.getElementById(`${type}-${symbol}`);

    if (!row) {
        row = tbody.insertRow();
        row.id = `${type}-${symbol}`;
    }

    const total = data.buyVol + data.sellVol;
    const bPerc = ((data.buyVol / total) * 100).toFixed(1);
    const sPerc = ((data.sellVol / total) * 100).toFixed(1);
    const agg = bPerc > 52 ? 'COMPRA 🚀' : (sPerc > 52 ? 'VENDA 🔥' : 'NEUTRO');

    row.innerHTML = `
        <td>${symbol}</td>
        <td>$${data.price.toFixed(4)}</td>
        <td class="buy">${bPerc}%</td>
        <td class="sell">${sPerc}%</td>
        <td><strong>${agg}</strong></td>
        <td><button class="alert-btn" onclick="setQuickAlert('${symbol}', ${data.price})">🔔 Alert</button></td>
    `;
}

// Lógica de Alertas
function setQuickAlert(symbol, currentPrice) {
    const target = prompt(`Definir alerta para ${symbol}. Preço atual: ${currentPrice}\nDigite o preço alvo (acima ou abaixo):`);
    if (target) {
        state.alerts.push({
            symbol,
            target: parseFloat(target),
            direction: parseFloat(target) > currentPrice ? 'UP' : 'DOWN'
        });
        alert(`Alerta definido para ${symbol} em $${target}`);
    }
}

function checkPriceAlerts(symbol, price) {
    state.alerts.forEach((alert, index) => {
        if (alert.symbol === symbol) {
            if ((alert.direction === 'UP' && price >= alert.target) || 
                (alert.direction === 'DOWN' && price <= alert.target)) {
                
                alert(`🚨 ALERTA: ${symbol} cruzou ${alert.target}! Preço atual: ${price}`);
                state.alerts.splice(index, 1);
            }
        }
    });
}

function switchTab(tab) {
    document.getElementById('spotSection').classList.toggle('hidden', tab !== 'spot');
    document.getElementById('futuresSection').classList.toggle('hidden', tab !== 'futures');
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.innerText.toLowerCase().includes(tab)));
}