var active_timeframe = sessionStorage.getItem("timeframe");
if (!active_timeframe) active_timeframe = "1d";
var saved_compares = sessionStorage.getItem("compares");
var active_compares = new Set();
if (saved_compares !== null) {
    for (const symbol of JSON.parse(saved_compares)) {
        active_compares.add(symbol);
    }
}

var histories = INIT_HISTORIES;
var watch = INIT_WATCH;
var orders = INIT_ORDERS;
var positions = INIT_POSITIONS;
const graph_timeframes = ["1d", "1w", "2w", "1m", "6m", "ytd", "1y", "5y"];
const watch_symbols = ["QQQ", "SPY", "DIA", "TQQQ"];
const compare_symbols = ["qqq", "spy", "tqqq"];

const graph_point_radius = {
    "1d": 0, "1w": 3, "2w": 3, "1m": 2,
    "6m": 0, "ytd": 0, "1y": 0, "5y": 0
};
const symbol_colors = {
    "qqq": "rgb(11, 166, 188)",
    "qqqlight": "rgba(11, 166, 188, 0.25)",
    "spy": "rgb(215, 150, 40)",
    "spylight": "rgba(215, 150, 40, 0.25)",
    "tqqq": "rgb(170, 70, 190)",
    "tqqqlight": "rgba(170, 70, 190, 0.25)",
};

function change_timeframe(timeframe) {
    document.getElementById("btn-" + active_timeframe).classList.remove("active");
    document.getElementById("btn-" + timeframe).classList.add("active");
    document.getElementById("current-change").innerHTML = histories["change_" + timeframe];
    active_timeframe = timeframe;
    sessionStorage.setItem("timeframe", active_timeframe);
}
change_timeframe(active_timeframe);

function change_compare(symbol) {
    document.getElementById("btn-" + symbol).classList.toggle("active");
    if (active_compares.has(symbol)) {
        active_compares.delete(symbol);
    } else {
        active_compares.add(symbol);
    }
    sessionStorage.setItem("compares", JSON.stringify(Array.from(active_compares)));
}
for (const symbol of active_compares.values()) {
    document.getElementById("btn-" + symbol).classList.add("active");
}

var graph_data = {};
var symbol_data = {};

const config = {
    type: "line",
    data: {},
    options: {
        maintainAspectRatio: false,
        interaction: {
            intersect: false
        },
        plugins: {
            legend: {
                display: false
            },
            tooltip: {
                callbacks: {
                    afterLabel: (ctx) => {
                        if (ctx.dataset.label === "previous close") {
                            return;
                        }
                        var base;
                        if (active_timeframe === "1d") {
                            base = histories["prev_close"];
                        } else {
                            base = ctx.dataset.data[0];
                        }
                        var change = (ctx.raw / base - 1) * 100;
                        var sign = change >= 0 ? "+" : "-";
                        return `${sign}${(Math.abs(change)).toFixed(2)}%`;
                    }
                }
            }
        },
        scales: {
            x: {
                ticks: {
                    autoSkip: true,
                    autoSkipPadding: 15,
                    maxRotation: 0
                }
            }
        },
        spanGaps: true
    }
};
const chart = new Chart(document.getElementById("portfolio-graph"), config);
function update_chart() {
    chart.data = {
        labels: histories["time_" + active_timeframe],
        datasets: graph_data[active_timeframe].slice()
    }
    for (const symbol of active_compares.values()) {
        chart.data.datasets.push(symbol_data[active_timeframe][symbol]);
    }
    chart.update();
}

for (const timeframe of graph_timeframes) {
    document.getElementById("btn-" + timeframe).addEventListener("click", () => {
        if (active_timeframe === timeframe) {
            return;
        }
        change_timeframe(timeframe);
        update_chart();
    });
}
for (const symbol of compare_symbols) {
     document.getElementById("btn-" + symbol).addEventListener("click", () => {
        change_compare(symbol);
        update_chart();
    });
}

function get_histories_hash(his) {
    var time_1d = his["time_1d"];
    var time_1w = his["time_1w"];
    var equity_1d = his["equity_1d"];
    return time_1w[time_1w.length - 1] + time_1d[time_1d.length - 1] + equity_1d[equity_1d.length - 1];
}

// Initial load of graph_data and symbol_data
function init_histories() {
    document.getElementById("current-equity").innerHTML = histories["current_equity"];
    for (const timeframe of graph_timeframes) {
        graph_data[timeframe] = [{
            label: "value",
            backgroundColor: histories["color_" + timeframe],
            borderColor: histories["color_" + timeframe],
            borderWidth: 2,
            radius: graph_point_radius[timeframe],
            data: histories["equity_" + timeframe]
        }];
    }
    const light_color = histories["color_1d"] === "red" ? "rgba(252, 0, 0, 0.25)" : "rgba(0, 128, 0, 0.25)";
    graph_data["1d"][0].segment = {
        borderColor: ctx => histories["time_1d"][ctx.p1DataIndex] > histories['market_close'] ? light_color : undefined
    };
    graph_data["1d"].push({
        label: "previous close",
        borderColor: 'grey',
        borderDash: [6, 6],
        borderWidth: 1,
        radius: 0,
        data: Array(histories["time_1d"].length).fill(histories["prev_close"])
    });
    for (const timeframe of graph_timeframes) {
        symbol_data[timeframe] = {};
        for (const symbol of compare_symbols) {
            dataset = {
                label: symbol.toUpperCase(),
                backgroundColor: symbol_colors[symbol],
                borderColor: symbol_colors[symbol],
                borderWidth: 2,
                radius: graph_point_radius[timeframe],
                data: histories[symbol + "_" + timeframe]
            };
            if (timeframe === "1d") {
                dataset.segment = {
                    borderColor:
                     ctx => histories["time_1d"][ctx.p1DataIndex] > histories['market_close'] ? symbol_colors[symbol + "light"] : undefined
                };
            }
            symbol_data[timeframe][symbol] = dataset;
        }
    }
    update_chart();
}

// Update graph_data and symbol_data from most recent histories
function update_histories() {
    document.getElementById("current-equity").innerHTML = histories["current_equity"];
    document.getElementById("current-change").innerHTML = histories["change_" + active_timeframe];
    chart.data.labels = histories["time_" + active_timeframe];
    for (const timeframe of graph_timeframes) {
        var g_data = graph_data[timeframe][0];
        g_data.backgroundColor = histories["color_" + timeframe];
        g_data.borderColor = histories["color_" + timeframe];
        g_data.data = histories["equity_" + timeframe];
    }
    const light_color = histories["color_1d"] === "red" ? "rgba(252, 0, 0, 0.25)" : "rgba(0, 128, 0, 0.25)";
    graph_data["1d"][0].segment.borderColor = ctx => histories["time_1d"][ctx.p1DataIndex] > histories['market_close'] ? light_color : undefined;
    graph_data["1d"][1].data = Array(histories["time_1d"].length).fill(histories["prev_close"]);
    for (const timeframe of graph_timeframes) {
        for (const symbol of compare_symbols) {
            var s_data = symbol_data[timeframe][symbol];
            s_data.data = histories[symbol + "_" + timeframe];
            if (timeframe === "1d") {
                s_data.segment.borderColor = ctx => histories["time_1d"][ctx.p1DataIndex] > histories['market_close'] ? symbol_colors[symbol + "light"] : undefined;
            }
        }
    }
    chart.update();
}

function get_watch_hash(wat) {
    var hash = "";
    for (var symbol of watch_symbols) {
        hash += symbol + wat[symbol]["price"];
    }
    return hash;
}

function update_watch() {
    for (var symbol of watch_symbols) {
        document.getElementById(symbol + "-watch-price").innerHTML = watch[symbol]["price"];
        document.getElementById(symbol + "-watch-footnote").innerHTML = watch[symbol]["change"] + " " + watch[symbol]["date"];
    }
}

function get_orders_hash(ord) {
    var hash = "";
    for (var order of ord) {
        hash += `${order["symbol"]}-${order["side"]}-${order["time"]}`;
    }
    return hash;
}

function update_orders() {
    var html = "";
    for (var order of orders) {
        html += `<tr><td><a href=${order["link"]}>${order["symbol"]}</a></td>`
        var badge_style = "badge-purple";
        if (order["side"] === "buy") {
            badge_style = "badge-blue";
        }
        html += `<td><span class="badge-shape ${badge_style}">${order["side"]}</span></td>`
        html += `<td>${order["price"]}</td>`
        html += `<td class="xs-hidden">${order["value"]}</td>`
        html += `<td>${order["time"]}</td>`
        html += `<td>${order["gl"]}</td></tr>`
    }
    document.getElementById("orders-tbody").innerHTML = html;
}

function get_positions_hash(pos) {
    var hash = "";
    for (var position of pos) {
        hash += `${position["symbol"]}-${position["side"]}-${position["current_price"]}`;
    }
    return hash;
}

function update_positions() {
    var html = "";
    for (var position of positions) {
        html += `<tr><td><a href="${position["link"]}">${position["symbol"]}</a></td>`
        var badge_style = "badge-purple";
        if (position["side"] === "long") {
            badge_style = "badge-blue";
        }
        html += `<td><span class="badge-shape ${badge_style}">${position["side"]}</span></td>`
        html += `<td>${position["current_price"]}</td>`
        html += `<td>${position["value"]}</td>`
        html += `<td class="xs-hidden">${position["day_change"]}</td>`
        html += `<td>${position["gl"]}</td></tr>`
    }
    document.getElementById("positions-tbody").innerHTML = html;
}

// Initial load of data
var last_dashboard_update = new Date().getTime();
init_histories();
update_watch();
update_orders();
update_positions();

function update_dashboard_data() {
    if (document.hidden || new Date().getTime() - last_dashboard_update < 60000) {
        return;
    }
    last_dashboard_update = new Date().getTime();
    console.log(`Update dashboard at ${new Date()}`);
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open("GET", "/dashboard_data", true);
    xmlHttp.onreadystatechange = function() {
        if (xmlHttp.readyState !== 4 || xmlHttp.status !== 200) {
            return;
        }
        var res = xmlHttp.responseText;
        var obj = JSON.parse(res);
        if (get_histories_hash(histories) !==  get_histories_hash(obj.histories)) {
            histories = obj.histories;
            update_histories();
        }
        if (get_watch_hash(watch) !== get_watch_hash(obj.watch)) {
            watch = obj.watch;
            update_watch();
        }
        if (get_orders_hash(orders) !== get_orders_hash(obj.orders)) {
            orders = obj.orders;
            update_orders();
        }
        if (get_positions_hash(positions) != get_positions_hash(obj.positions)) {
            positions = obj.positions;
            update_positions();
        }
    }
    xmlHttp.send(null);
}

setInterval(update_dashboard_data, 1000);
