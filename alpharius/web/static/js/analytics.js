function get_dataset(timeframe, processor) {
    const values = GL_BARS["values"][timeframe][processor];
    var colors = [];
    for (value of values) {
        colors.push(value >= 0 ? "rgb(5,170,40)": "rgb(237,73,55)");
    }
    return {label: 'PnL', data: values, backgroundColor: colors};
}

const gl_config = {
    type: 'bar',
    data: {},
    options: {
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: false
            }
        },
        scales: {
             x: {
                ticks: {
                    autoSkip: true,
                    autoSkipPadding: 15,
                    maxRotation: 0,
                }
            },
            y: {
                beginAtZero: true
            }
        }
    },
};
const gl_chart = new Chart(document.getElementById("graph-gl-all"), gl_config);
const processor_select = document.getElementById("processor-select");
const timeframe_select = document.getElementById("timeframe-select");

function update_gl_chart() {
    gl_chart.data = {
        labels: GL_BARS["labels"][timeframe_select.value],
        datasets: [get_dataset(timeframe_select.value, processor_select.value)]
    }
    gl_chart.update();
}
update_gl_chart();
for (var select of [processor_select, timeframe_select]) {
    select.addEventListener("change", function(event){
        update_gl_chart();
    });
}

// Colors are assigned over every processor in every time range, so a processor keeps its color when the range changes.
var pie_chart_processors = [];
for (const entries of Object.values(CASH_FLOWS)) {
    for (const entry of entries) {
        if (!pie_chart_processors.includes(entry["processor"])) {
            pie_chart_processors.push(entry["processor"]);
        }
    }
}
const color_pool = ["#4890e8", "#4fdba8", "#915bde", "#fabe57", "#1bd1cf", "#eb624d",
                    "#8f8f7f", "#eb57cd", "#d1ce6d", "#8097b0", "#2d41f7", "#ed2f72",
                    "#146e10", "#7b8a75"];
const color_assignments = {};
for (var i = 0; i < pie_chart_processors.length; i++) {
    color_assignments[pie_chart_processors[i]] = color_pool[i];
}
function pie_data(entries, value_key) {
    return {
        labels: entries.map(entry => entry["processor"]),
        datasets: [{
            label: '',
            data: entries.map(entry => entry[value_key]),
            backgroundColor: entries.map(entry => color_assignments[entry["processor"]])
        }]
    };
}
const pie_chart_config = {
    type: "pie",
    options: {
        maintainAspectRatio: false,
        responsive: true,
        plugins: {
            legend: {position: "top"}
        }
    }
};
var cash_flow_config = Object.assign({}, pie_chart_config);
cash_flow_config.data = pie_data(CASH_FLOWS[DEFAULT_STATS_RANGE], "cash_flow");
const cash_flow_chart = new Chart(document.getElementById("graph-cash-flow"), cash_flow_config);

// Time range: the profit and slippage tables show the table body of the range and the cash flow pie redraws.
// Every card has its own select, and they stay in sync.
const range_selects = document.querySelectorAll(".range-select");
function show_stats_range(range) {
    for (const body of document.querySelectorAll("tbody[data-range]")) {
        body.classList.toggle("hidden", body.dataset.range !== range);
    }
    cash_flow_chart.data = pie_data(CASH_FLOWS[range], "cash_flow");
    cash_flow_chart.update();
    for (const select of range_selects) {
        select.value = range;
    }
}
for (const select of range_selects) {
    select.addEventListener("change", () => show_stats_range(select.value));
}

var annual_return_datasets = [];
const symbol_colors = {
    "my portfolio": "rgb(62, 110, 186)",
    "qqq": "rgb(11, 166, 188)",
    "spy": "rgb(215, 150, 40)",
};
for (var i = 0; i < ANNUAL_RETURN["symbols"].length; i++) {
    const symbol = ANNUAL_RETURN["symbols"][i];
    annual_return_datasets.push({
        label: symbol,
        data: ANNUAL_RETURN["returns"][i],
        backgroundColor: symbol_colors[symbol.toLowerCase()]
    });
}
const annual_return_config = {
    type: "bar",
    data: {
        labels: ANNUAL_RETURN["years"],
        datasets: annual_return_datasets
    },
    options: {
        maintainAspectRatio: false,
        responsive: true,
        plugins: {
            legend: {position: "top"},
            tooltip: {
                callbacks: {
                    label: function(context) {
                        let label = context.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (context.parsed.y !== null) {
                            label += context.parsed.y.toFixed(2) + '%';
                        }
                        return label;
                    }
                }
            }
        }
    },
};
const annual_return_chart = new Chart(document.getElementById("graph-annual-return"), annual_return_config);
