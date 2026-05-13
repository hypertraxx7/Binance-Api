// app.js
const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT']; // Lista resumida para o exemplo
const marketData = {};
const alerts = [];

// Conexão via WebSocket (Streams de Agregado de Trades)
const socket = new WebSocket('wss://stream.binance.com:9443/ws');

const subscribeMsg = {
    method: "SUBSCRIBE",
    params: symbols.map(s => `${s.toLowerCase()}@aggTrade`),
    id: 1
};

socket.onopen = () => {
    socket.send(JSON.stringify(subscribeMsg));
    console.log("Conectado à Binance WebSocket");
};

socket.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.e === 'aggTrade') {
        updateMarketData(data);
    }
};

function updateMarketData(trade) {
    const symbol = trade.s;
    const price = parseFloat(trade.p);
    const quantity = parseFloat(trade.q);
    const isBuyerMaker = trade.m; // m: true = Vendedor agressivo, m: false = Comprador agressivo

    if (!marketData[symbol]) {
        marketData[symbol] = { buyVol: 0, sellVol: 0, price: 0 };
    }

    marketData[symbol].price = price;
    
    if (isBuyerMaker) {
        marketData[symbol].sellVol += quantity;
    } else {
        marketData[symbol].buyVol += quantity;
    }

    renderTable();
    checkAlerts(symbol, price);
}

function renderTable() {
    const tbody = document.getElementById('marketBody');
    tbody.innerHTML = '';

    Object.keys(marketData).forEach(symbol => {
        const data = marketData[symbol];
        const totalVol = data.buyVol + data.sellVol;
        const buyRatio = (data.buyVol / totalVol * 100).toFixed(1);
        const sellRatio = (data.sellVol / totalVol * 100).toFixed(1);
        
        const side = data.buyVol > data.sellVol ? 'Compradores' : 'Vendedores';
        const aggressive = buyRatio > 55 ? 'Compra Forte' : (sellRatio > 55 ? 'Venda Forte' : 'Equilibrado');

        const row = `
            <tr>
                <td>**${symbol}**</td>
                <td>$${data.price.toLocaleString()}</td>
                <td class="buy">${buyRatio}%</td>
                <td class="sell">${sellRatio}%</td>
                <td class="aggressive">${aggressive}</td>
                <td>${side} movimentando</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// Sistema de Alertas
function setAlert() {
    const symbol = document.getElementById('alertSymbol').value.toUpperCase();
    const min = parseFloat(document.getElementById('minPrice').value);
    const max = parseFloat(document.getElementById('maxPrice').value);

    alerts.push({ symbol, min, max });
    document.getElementById('activeAlerts').innerHTML += `<p>Alerta: ${symbol} (${min} - ${max})</p>`;
}

function checkAlerts(symbol, price) {
    alerts.forEach((alert, index) => {
        if (alert.symbol === symbol) {
            if (price <= alert.min || price >= alert.max) {
                alertNotification(symbol, price);
                alerts.splice(index, 1); // Remove após disparar
            }
        }
    });
}

function alertNotification(symbol, price) {
    alert(`ALERTA: ${symbol} atingiu o preço de $${price}!`);
}