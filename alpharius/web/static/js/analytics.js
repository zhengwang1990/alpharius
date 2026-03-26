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

var pie_chart_processors = [];
var trans_label = [];
var trans_value = [];
var trans_color = [];
for (var entry of TRANSACTION_CNT) {
    trans_label.push(entry["processor"]);
    trans_value.push(entry["cnt"]);
    if (!pie_chart_processors.includes(entry["processor"])) {
        pie_chart_processors.push(entry["processor"]);
    }
}
var cash_flow_label = [];
var cash_flow_value = [];
var cash_flow_color = [];
for (var entry of CASH_FLOWS) {
    cash_flow_label.push(entry["processor"]);
    cash_flow_value.push(entry["cash_flow"]);
    if (!pie_chart_processors.includes(entry["processor"])) {
        pie_chart_processors.push(entry["processor"]);
    }
}
const color_pool = ["#4890e8", "#4fdba8", "#915bde", "#fabe57", "#1bd1cf", "#eb624d",
                    "#8f8f7f", "#eb57cd", "#d1ce6d", "#8097b0", "#2d41f7", "#ed2f72",
                    "#146e10", "#7b8a75"];
const color_assignments = {};
for (var i = 0; i < pie_chart_processors.length; i++) {
    color_assignments[pie_chart_processors[i]] = color_pool[i];
}
for (var processor of trans_label) {
    trans_color.push(color_assignments[processor]);
}
for (var processor of cash_flow_label) {
    cash_flow_color.push(color_assignments[processor]);
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
var trans_cnt_config = Object.assign({}, pie_chart_config);
trans_cnt_config.data = {
    labels: trans_label,
    datasets: [{label: '', data: trans_value, backgroundColor: trans_color}]
};
const trans_chart = new Chart(document.getElementById("graph-trans-cnt"), trans_cnt_config);
var cash_flow_config = Object.assign({}, pie_chart_config);
cash_flow_config.data = {
    labels: cash_flow_label,
    datasets: [{label: '', data: cash_flow_value, backgroundColor: cash_flow_color}]
};
const cash_flow_chart = new Chart(document.getElementById("graph-cash-flow"), cash_flow_config);

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
