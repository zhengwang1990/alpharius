const intraday_datepicker = new Datepicker(document.getElementById("intraday-datepicker"), {
    autohide: true,
    format: "yyyy-mm-dd",
    maxDate: new Date(),
    daysOfWeekDisabled: [0, 6]
});

const daily_datepicker = new DateRangePicker(document.getElementById("daily-datepicker"), {
    autohide: true,
    format: "yyyy-mm-dd",
    maxDate: new Date(),
    daysOfWeekDisabled: [0, 6]
});

// Global variables
var chart_mode = null;
var intraday_chart_data = null;
var trimmed_intraday_chart_data = null;
var daily_chart_data = null;
var intraday_chart = null;
var daily_chart = null;
var symbol_tree = {symbols: [], children: {}};
var historical_symbols = [];
var historical_dates = [];
var historical_entries = [];
var historical_exits = [];
const symbol_set = new Set(ALL_SYMBOLS);
const intraday_alert = document.getElementById("intraday-alert");
const intraday_symbol_input = document.getElementById("intraday-symbol-input");
const intraday_entry_input = document.getElementById("intraday-entry-input");
const intraday_exit_input = document.getElementById("intraday-exit-input");
const intraday_chart_container = document.getElementById("intraday-chart-container");
const intraday_chart_name = document.getElementById("intraday-chart-name");
const intraday_button = document.getElementById("intraday-chart-btn");
const daily_alert = document.getElementById("daily-alert");
const daily_symbol_input = document.getElementById("daily-symbol-input");
const daily_chart_container = document.getElementById("daily-chart-container");
const daily_chart_name = document.getElementById("daily-chart-name");
const daily_button = document.getElementById("daily-chart-btn");
const history_card = document.getElementById("history-card");
const history_container = document.getElementById("history-container");

if (window.innerWidth <= 800) {
    chart_mode = "compact";
} else {
    chart_mode = "full";
}

function drawLine(ctx, startX, startY, endX, endY) {
    ctx.beginPath();
    ctx.moveTo(startX, startY);
    ctx.lineTo(endX, endY);
    ctx.stroke();
    ctx.closePath();
}

const candlestick = {
    id: "candlestick",
    beforeDatasetsDraw: ((chart, args, pluginOptions) => {
        const { ctx, data, scales: { y } } = chart;
        ctx.save();
        ctx.lineWidth = chart_mode === "compact" ? 0.5 : 1;
        ctx.strokeStyle = "black";

        data.datasets[0].data.forEach((dataPoint, index) => {
            const bar_top = Math.max(dataPoint.c, dataPoint.o);
            const bar_bottom = Math.min(dataPoint.c, dataPoint.o);
            const x = chart.getDatasetMeta(0).data[index].x;

            drawLine(ctx,
                     x, y.getPixelForValue(bar_top),
                     x, y.getPixelForValue(dataPoint.h));

            drawLine(ctx,
                     x, y.getPixelForValue(bar_bottom),
                     x, y.getPixelForValue(dataPoint.l));

            if (bar_top === bar_bottom) {
                const bar_width = chart.getDatasetMeta(0).data[index].width;
                drawLine(ctx,
                         x - bar_width / 2, y.getPixelForValue(bar_top),
                         x + bar_width / 2, y.getPixelForValue(bar_top));
            }
        });
    })
};

const barPosition = {
    id: "barPosition",
    beforeDatasetsDraw: ((chart, args, pluginOptions) => {
        const { ctx, data, chartArea: { left, width }, scales: { x } } = chart;
        const bar_width = width / data.labels.length;
        for (var i of [0, 1]) {
            chart.getDatasetMeta(i).data.forEach((datapoint, index) => {
                datapoint.x = left + bar_width * (index + 0.5);
            });
        }
    })
};

const crosshair = {
    id: "crosshair",
    beforeDatasetsDraw: ((chart, args, pluginOptions) => {
        const { ctx, data, tooltip, chartArea: { top, bottom, left, right }, scales: { y } } = chart;
        if (tooltip._active && tooltip._active.length && tooltip.dataPoints[0].raw.c) {
            const activePoint = tooltip._active[0];
            const closeValue = tooltip.dataPoints[0].raw.c;
            ctx.setLineDash([3, 3]);
            ctx.lineWidth = 1;
            ctx.strokeStyle = "rgb(102, 102, 102)";

            drawLine(ctx,
                     activePoint.element.x, top,
                     activePoint.element.x, bottom);

            drawLine(ctx,
                     left, y.getPixelForValue(closeValue),
                     right, y.getPixelForValue(closeValue));
            ctx.setLineDash([]);
        }
    })
};

const MARK_STYLES = {
    entry: {color: "rgb(2, 132, 199)", label: "ENTRY"},
    exit: {color: "rgb(124, 58, 237)", label: "EXIT"},
    mark: {color: "rgb(23, 23, 24)", label: ""},
};

// Height of the strip above the plot that holds the ENTRY / EXIT labels.
function mark_label_space() {
    return chart_mode === "compact" ? 22 : 26;
}

// Finds the bar each entry/exit mark falls in and returns its pixel position and style.
// Bars are 5 minutes long and labeled by start time, so 10:42 belongs to the 10:40 bar.
function get_mark_bars(chart, mark_points) {
    const { data, chartArea: { left, width } } = chart;
    const bar_width = width / data.labels.length;
    const to_minutes = (t) => parseInt(t.slice(0, 2)) * 60 + parseInt(t.slice(3, 5));
    const res = [];
    if (!Array.isArray(mark_points)) {
        return res;
    }
    const bars = data.datasets[0].data;
    for (const mark of mark_points) {
        const index = bars.findIndex(bar => {
            const diff = to_minutes(mark.time) - to_minutes(bar.x);
            return diff >= 0 && diff < 5;
        });
        if (index >= 0) {
            res.push({
                dataPoint: bars[index],
                x: left + bar_width * (index + 0.5),
                bar_width: bar_width,
                style: MARK_STYLES[mark.role] || MARK_STYLES.mark,
            });
        }
    }
    return res;
}

// Horizontal positions for the ENTRY / EXIT labels. Each is centered on its line; if two would overlap,
// the earlier one ends at its line and the later one starts at its line, like two flags.
function layout_mark_labels(labels, left, right) {
    const gap = 3;
    const clamp = (l, width) => Math.min(Math.max(l, left), right - width);
    labels.sort((a, b) => a.x - b.x);
    for (const label of labels) {
        label.left = clamp(label.x - label.width / 2, label.width);
    }
    for (let i = 1; i < labels.length; i++) {
        const prev = labels[i - 1];
        const cur = labels[i];
        if (cur.left < prev.left + prev.width + gap) {
            prev.left = clamp(prev.x - prev.width, prev.width);
            cur.left = clamp(cur.x, cur.width);
            if (cur.left < prev.left + prev.width + gap) {
                prev.left = cur.left - gap - prev.width;
            }
        }
    }
}

const closePointer = {
    id: "closePointer",

    // Dashed vertical line behind the candles, full chart height.
    beforeDatasetsDraw: ((chart, args, pluginOptions) => {
        const { ctx, chartArea: { top, bottom } } = chart;
        for (const bar of get_mark_bars(chart, pluginOptions.mark_points)) {
            ctx.save();
            ctx.strokeStyle = bar.style.color;
            ctx.lineWidth = 1;
            ctx.setLineDash([4, 3]);
            drawLine(ctx, bar.x, top, bar.x, bottom);
            ctx.restore();
        }
    }),

    // Triangle at the close price plus an ENTRY / EXIT tag above the plot.
    afterDatasetsDraw: ((chart, args, pluginOptions) => {
        const { ctx, chartArea: { top, left, right }, scales: { y } } = chart;
        const compact = chart_mode === "compact";
        const font_size = compact ? 9 : 11;
        const pad = 4;
        const label_height = font_size + 2 * pad - 2;
        const label_y = top - label_height - 3;
        const labels = [];
        ctx.save();
        ctx.font = `600 ${font_size}px sans-serif`;
        for (const bar of get_mark_bars(chart, pluginOptions.mark_points)) {
            const { dataPoint, x: xc, style } = bar;
            const yc = y.getPixelForValue(dataPoint.c);
            // On mobile the triangle is never wider than a bar.
            const size = compact ? bar.bar_width : Math.max(0.9 * bar.bar_width, 6);
            const dir = dataPoint.c < dataPoint.o ? 1 : -1;

            ctx.fillStyle = style.color;
            ctx.beginPath();
            ctx.moveTo(xc, yc);
            ctx.lineTo(xc - 0.5 * size, yc + dir * 0.9 * size);
            ctx.lineTo(xc + 0.5 * size, yc + dir * 0.9 * size);
            ctx.closePath();
            ctx.fill();

            if (style.label) {
                labels.push({x: xc, width: ctx.measureText(style.label).width + 2 * pad, style: style});
            }
        }
        layout_mark_labels(labels, left, right);
        ctx.textBaseline = "middle";
        for (const label of labels) {
            ctx.fillStyle = label.style.color;
            ctx.beginPath();
            if (ctx.roundRect) {
                ctx.roundRect(label.left, label_y, label.width, label_height, 3);
            } else {
                ctx.rect(label.left, label_y, label.width, label_height);
            }
            ctx.fill();
            ctx.fillStyle = "white";
            ctx.fillText(label.style.label, label.left + pad, label_y + label_height / 2 + 0.5);
        }
        ctx.restore();
    })
};

function displayAlert(type, message, timeframe) {
    var chart_container, chart_name, alert;
    if (timeframe === "intraday") {
        chart_container = intraday_chart_container;
        chart_name = intraday_chart_name;
        alert = intraday_alert;
    } else {
        chart_container = daily_chart_container;
        chart_name = daily_chart_name;
        alert = daily_alert;
    }
    chart_container.style.display = "none";
    chart_name.style.display = "none";
    for (c of alert.classList.values()) {
        if (c != "alert") {
            alert.classList.remove(c);
        }
    }
    alert.classList.add("alert-" + type);
    alert.innerHTML = message;
    alert.style.removeProperty("display");
}

function get_chart_data(dates, symbol, timeframe, marks=null) {
    var xmlHttp = new XMLHttpRequest();
    var route;
    if (timeframe === "intraday") {
        route = `/charts_data?date=${dates[0]}&symbol=${symbol}&timeframe=intraday`
        if (marks !== null) {
            route += `&marks=${marks}`
        }
    } else {
        route = `/charts_data?start_date=${dates[0]}&end_date=${dates[1]}&symbol=${symbol}&timeframe=daily`
    }
    xmlHttp.open("GET", route, false);
    xmlHttp.send(null);
    var res = xmlHttp.responseText;
    var obj;
    try {
        obj = JSON.parse(res);
    } catch (e) {
        var error_message = new DOMParser().parseFromString(res, "text/html").getElementsByClassName("errormsg");
        if (error_message.length > 0) {
            displayAlert("danger", error_message[0].innerHTML, timeframe);
        } else {
            var p = document.createElement("p");
            p.appendChild(document.createTextNode(e.toString()));
            displayAlert("danger", p.innerHTML, timeframe);
        }
        throw e;
    }
    if (timeframe === "intraday") {
        intraday_chart_data = obj;
        trimmed_intraday_chart_data = {
            labels: [],
            prices: [],
            volumes: [],
            name: intraday_chart_data['name'],
            prev_close: intraday_chart_data["prev_close"]
        }
        if (obj.marks !== undefined) {
            trimmed_intraday_chart_data.marks = obj.marks;
        }
        for (var i = 0; i < intraday_chart_data["labels"].length; i++) {
            var label = intraday_chart_data["labels"][i];
            if (label >= "09:30" && label < "16:00") {
                trimmed_intraday_chart_data.labels.push(label);
                trimmed_intraday_chart_data.prices.push(intraday_chart_data["prices"][i]);
                trimmed_intraday_chart_data.volumes.push(intraday_chart_data["volumes"][i]);
            }
        }
    } else {
        daily_chart_data = obj;
    }
}

// Returns "HH:MM" for inputs like "9:45" or "09:45:00", or null if invalid.
function normalize_time(text) {
    const m = text.trim().match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
    if (m === null || parseInt(m[1]) > 23 || parseInt(m[2]) > 59) {
        return null;
    }
    return `${m[1].padStart(2, "0")}:${m[2]}`;
}

// Entry/exit inputs as a comma separated marks string, or null if both are empty.
// Also records which of the marks is the entry and which the exit, in the same order.
var intraday_mark_roles = [];
function get_marks() {
    const marks = [];
    const roles = [];
    for (const [role, input] of [["entry", intraday_entry_input], ["exit", intraday_exit_input]]) {
        const time = normalize_time(input.value);
        if (time !== null) {
            marks.push(time);
            roles.push(role);
        }
    }
    intraday_mark_roles = roles;
    return marks.length > 0 ? marks.join(",") : null;
}

// Fills the intraday inputs without querying.
function fill_intraday_inputs(symbol, date, entry, exit) {
    intraday_symbol_input.value = symbol;
    intraday_entry_input.value = entry || "";
    intraday_exit_input.value = exit || "";
    const date_utc = Date.parse(date);
    intraday_datepicker.setDate(date_utc + (new Date(date_utc).getTimezoneOffset() * 60000));
    if (button_state === 1) {
        toggle_button_state();
    }
}

function get_chart(timeframe) {
    var dates, symbol_input;
    if (timeframe === "intraday") {
        var date = intraday_datepicker.getDate("yyyy-mm-dd");
        if (date === undefined) {
            displayAlert("danger", "Date must be selected", timeframe);
            return 1;
        }
        if (!validateDate(date)) {
            displayAlert("danger", `${date} is not a valid date`, timeframe);
            return 1;
        }
        dates = [date];
        symbol_input = intraday_symbol_input;
        for (var time_input of [intraday_entry_input, intraday_exit_input]) {
            const time_text = time_input.value.trim();
            if (time_text === "") {
                continue;
            }
            const time = normalize_time(time_text);
            if (time === null) {
                displayAlert("danger", `${time_text} is not a valid time, expected HH:MM`, timeframe);
                return 1;
            }
            if (parseInt(time.slice(3)) % 5 !== 0) {
                displayAlert("danger", `${time_text} is not a multiple of 5 minutes, e.g. 10:45`, timeframe);
                return 1;
            }
        }
        const entry_time = normalize_time(intraday_entry_input.value);
        const exit_time = normalize_time(intraday_exit_input.value);
        if (entry_time !== null && exit_time !== null && entry_time >= exit_time) {
            displayAlert("danger", `Entry time ${entry_time} must be earlier than exit time ${exit_time}`, timeframe);
            return 1;
        }
    } else {
        dates = daily_datepicker.getDates("yyyy-mm-dd");
        for (var date of dates) {
            if (date === undefined) {
                displayAlert("danger", "Date must be selected", timeframe);
                return 1;
            }
            if (!validateDate(date)) {
                displayAlert("danger", `${date} is not a valid date`, timeframe);
                return 1;
            }
        }
        symbol_input = daily_symbol_input;
    }
    var symbol = symbol_input.value.toUpperCase();
    if (symbol.length === 0) {
        displayAlert("danger", "Symbol must be entered", timeframe);
        return 1;
    }
    if (!validateSymbol(symbol)) {
        displayAlert("danger", `${symbol} is not a valid symbol`, timeframe);
        return 1;
    }
    get_chart_data(dates, symbol, timeframe, timeframe === "intraday" ? get_marks() : null);
    update_chart(timeframe);
    // Auto set daily input boxes
    if (timeframe === "intraday" && daily_chart_container.style.display === "none") {
        copy_date_to_daily();
    }
    return 0;
}

function copy_date_to_daily() {
    daily_symbol_input.value = intraday_symbol_input.value.toUpperCase();
    var date = intraday_datepicker.getDate("yyyy-mm-dd");
    var date_utc = Date.parse(date);
    var start_date = date_utc + (new Date(date_utc).getTimezoneOffset() * 60000);
    start_date -= 86400000 * 92;
    var day = new Date(start_date).getDay();
    if (day === 0 || day === 6) {
        start_date += 86400000 * 2;
    }
    var end_date = date_utc + (new Date(date_utc).getTimezoneOffset() * 60000);
    daily_datepicker.setDates(start_date, end_date);
}

function update_chart(timeframe) {
    var current_data;
    if (timeframe === "intraday") {
        current_data = chart_mode === "compact" ? trimmed_intraday_chart_data : intraday_chart_data;
    } else {
        current_data = daily_chart_data;
    }
    var prices = current_data["prices"];
    if (prices.length === 0) {
        displayAlert("secondary", "No data found", timeframe);
        return;
    }
    var price_max = prices.reduce((res, elem) => Math.max(res, elem.h), -Infinity);
    var price_min = prices.reduce((res, elem) => Math.min(res, elem.l), Infinity);
    if (timeframe === "intraday") {
        price_max = Math.max(price_max, current_data["prev_close"]);
        price_min = Math.min(price_min, current_data["prev_close"]);
    }
    var mark_points = [];
    if (timeframe === "intraday" && current_data.marks !== undefined) {
        current_data.marks.forEach((time, i) => {
            mark_points.push({time: time, role: intraday_mark_roles[i] || "mark"});
        });
    }
    const data = {
        labels: current_data["labels"],
        datasets: [{
            data: prices,
            backgroundColor: (ctx) => {
                const { raw: {x, o, c} } = ctx;
                let color, alpha;
                if (timeframe === "intraday" && (x < "09:30" || x >= "16:00")) {
                    alpha = 0.3;
                } else {
                    alpha = 0.9;
                }
                if (c >= o) {
                    color = `rgba(5, 170, 40, ${alpha})`;
                } else {
                    color = `rgba(237, 73, 55, ${alpha})`;
                }
                return color;
            },
            borderColor: "black",
            borderWidth: chart_mode === "compact" ? 0.5 : 1,
            borderSkipped: false,
            barPercentage: 1.5,
            categoryPercentage: 1,
            yAxisID: "y"
        }, {
           data: current_data["volumes"],
                backgroundColor: (ctx) => {
                const { raw: {x, g} } = ctx;
                let color, alpha;
                if (timeframe === "intraday" && (x < "09:30" || x >= "16:00")) {
                    alpha = 0.3;
                } else {
                    alpha = 0.9;
                }
                if (g == 1) {
                    color = `rgba(5, 170, 40, ${alpha})`;
                } else {
                    color = `rgba(237, 73, 55, ${alpha})`;
                }
                return color;
            },
            borderColor: "black",
            borderWidth: chart_mode === "compact" ? 0.5 : 1,
            yAxisID: "yLower",
            barPercentage: 1.5,
            categoryPercentage: 1,
        }]
    };
    var chart_annotations = [];
    if (timeframe === "intraday") {
        var position = "end";
        var prev_close = current_data["prev_close"];
        if (prices.length > 5) {
            var startDistance = 0;
            var endDistance = 0;
            for (var i = 0; i < 5; i++) {
                startDistance += Math.abs(prices[i].c - prev_close);
                endDistance += Math.abs(prices[prices.length - 1 - i].c - prev_close);
            }
            if (startDistance > endDistance) {
                position = "start";
            }
        }
        const prev_close_annotation = {
            type: "line",
            drawTime: "beforeDatasetsDraw",
            borderWidth: chart_mode === "compact" ? 0.5 : 1,
            borderColor: "rgba(141, 141, 141, 0.5)",
            borderDash: [6, 6],
            scaleID: "y",
            value: current_data["prev_close"],
            label: {
                display: chart_mode === "full",
                backgroundColor: "rgba(100, 100, 100, 0.7)",
                content: `previous close ${prev_close.toFixed(2)}`,
                position: position
            }
        };
        chart_annotations.push(prev_close_annotation);
    }
    var plugins = [candlestick, barPosition, crosshair];
    if (timeframe === "intraday") {
        plugins.push(closePointer);
    }
    const chart_config = {
        type: "bar",
        data: data,
        options: {
            maintainAspectRatio: false,
            layout: {
                padding: {top: mark_points.length > 0 ? mark_label_space() : 0}
            },
            interaction: {
                intersect: false
            },
            parsing: {
                yAxisKey: "s"
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        beforeBody: (ctx) => {
                            if (ctx[0].raw.o !== undefined) {
                                return [
                                    `O: ${ctx[0].raw.o.toFixed(2)}`,
                                    `H: ${ctx[0].raw.h.toFixed(2)}`,
                                    `L: ${ctx[0].raw.l.toFixed(2)}`,
                                    `C: ${ctx[0].raw.c.toFixed(2)}`
                                ];
                            } else {
                                return `Volume: ${ctx[0].raw.s}`
                            }
                        },
                        label: (ctx) => {
                            return '';
                        }
                    }
                },
                annotation: {
                    annotations: chart_annotations
                },
                closePointer: {
                    mark_points: mark_points
                }
            },
            scales: {
                x: {
                    ticks: {
                        autoSkip: true,
                        autoSkipPadding: 15,
                        maxRotation: 0
                    }
                },
                yLower: {
                    beginAtZero: true,
                    stack: "yScale",
                    stackWeight: 1,
                    ticks: {
                        display: false
                    }
                },
                y: {
                    beginAtZero: false,
                    stack: "yScale",
                    stackWeight: 4,
                    suggestedMax: price_max,
                    suggestedMin: price_min
                }
            }
        },
        plugins: plugins
    };
    var alert, chart_container, chart, chart_name;
    if (timeframe === "intraday") {
        alert = intraday_alert;
        chart_container = intraday_chart_container;
        chart = intraday_chart;
        chart_name = intraday_chart_name;
    } else {
        alert = daily_alert;
        chart_container = daily_chart_container;
        chart = daily_chart;
        chart_name = daily_chart_name;
    }
    alert.style.display = "none";
    chart_container.style.setProperty("--mark-extra", mark_points.length > 0 ? `${mark_label_space()}px` : "0px");
    chart_container.style.removeProperty("display");
    chart_name.style.removeProperty("display");
    chart_name.innerHTML = current_data["name"];
    if (chart === null) {
        if (timeframe === "intraday") {
            intraday_chart = new Chart(document.getElementById("graph-intraday-chart"), chart_config);
        } else {
            daily_chart = new Chart(document.getElementById("graph-daily-chart"), chart_config);
        }
    } else {
        chart.data = chart_config.data;
        chart.options = chart_config.options;
        chart.plugins = chart_config.plugins;
        chart.update();
    }
}

var button_state = 0;
function toggle_button_state() {
    if (button_state === 0) {
        button_state = 1;
    } else {
        button_state = 0;
    }
    document.getElementById("query-context").classList.toggle("hidden");
    document.getElementById("sync-context").classList.toggle("hidden");
    intraday_button.blur();
}
for (const input of [intraday_symbol_input, intraday_entry_input, intraday_exit_input]) {
    input.addEventListener("input", () => {
        if (button_state === 1) {
            toggle_button_state();
        }
    });
}
intraday_datepicker.element.addEventListener("changeDate", () => {
    if (button_state === 1) {
        toggle_button_state();
    }
});

intraday_button.addEventListener("click", () => {
    if (button_state === 0) {
        if (get_chart("intraday") === 0) {
            const query = {
                symbol: intraday_symbol_input.value.toUpperCase(),
                date: intraday_datepicker.getDate("yyyy-mm-dd"),
                entry: normalize_time(intraday_entry_input.value) || "",
                exit: normalize_time(intraday_exit_input.value) || "",
            };
            historical_symbols.push(query.symbol);
            historical_dates.push(query.date);
            historical_entries.push(query.entry);
            historical_exits.push(query.exit);
            save_last_query(query);
            toggle_button_state();
            if (historical_symbols.length >= 2) {
                const n = historical_symbols.length - 2;
                add_history_button({
                    symbol: historical_symbols[n],
                    date: historical_dates[n],
                    entry: historical_entries[n],
                    exit: historical_exits[n],
                });
            }
        }
    } else {
        copy_date_to_daily();
    }
});
daily_button.addEventListener("click", () => {get_chart("daily");});

window.addEventListener("resize", function(event) {
    const width = window.innerWidth;
    if (width <= 1600) {
        if (chart_mode !== "compact") {
            chart_mode = "compact";
            if (intraday_chart_container.style.display !== "none") {
                update_chart("intraday");
            }
            if (daily_chart_container.style.display !== "none") {
                update_chart("daily");
            }
        }
    } else {
        if (chart_mode !== "full") {
            chart_mode = "full";
            if (intraday_chart_container.style.display !== "none") {
                update_chart("intraday");
            }
            if (daily_chart_container.style.display !== "none") {
                update_chart("daily");
            }
        }
    }
}, true);

function validateDate(date) {
    return date.match(/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) !== null;
}

function validateSymbol(symbol) {
    return symbol_set.has(symbol);
}

if (validateDate(INIT_DATE) && validateSymbol(INIT_SYMBOL)) {
    var date_utc = Date.parse(INIT_DATE);
    intraday_datepicker.setDate(date_utc + (new Date(date_utc).getTimezoneOffset() * 60000));
    intraday_symbol_input.value = INIT_SYMBOL;
    // Links carry [entry, exit]. Anything else is drawn as plain marks without filling the boxes.
    const init_times = INIT_MARKS.split(",").map(normalize_time).filter(t => t !== null);
    let init_marks = null;
    if (init_times.length === 2) {
        [intraday_entry_input.value, intraday_exit_input.value] = init_times;
        init_marks = get_marks();
    } else if (init_times.length > 0) {
        intraday_mark_roles = [];
        init_marks = init_times.join(",");
    }
    get_chart_data([INIT_DATE], INIT_SYMBOL, "intraday", init_marks);
    update_chart("intraday");
    historical_symbols.push(INIT_SYMBOL);
    historical_dates.push(INIT_DATE);
    historical_entries.push(intraday_entry_input.value);
    historical_exits.push(intraday_exit_input.value);
} else {
    displayAlert("info", "Enter date and symbol. Then click QUERY.", "intraday");
}

if (validateDate(INIT_START_DATE) && validateDate(INIT_END_DATE) && validateSymbol(INIT_SYMBOL)) {
    var start_utc = Date.parse(INIT_START_DATE);
    var end_utc = Date.parse(INIT_END_DATE);
    daily_datepicker.setDates(start_utc + (new Date(start_utc).getTimezoneOffset() * 60000),
                              end_utc + (new Date(end_utc).getTimezoneOffset() * 60000));
    daily_symbol_input.value = INIT_SYMBOL;
    get_chart_data([INIT_START_DATE, INIT_END_DATE], INIT_SYMBOL, "daily");
    update_chart("daily");
} else {
    displayAlert("info", "Enter start date, end date and symbol. Then click QUERY.", "daily");
}

// Construct trie tree
for (var symbol of ALL_SYMBOLS) {
    var node = symbol_tree;
    for (var char of symbol) {
        if (node.children[char] === undefined) {
            node.children[char] = {symbols: [], children: {}}
        }
        node = node.children[char];
        if (node.symbols.length < 10) {
            node.symbols.push(symbol);
        }
    }
}

function add_auto_complete(symbol_input) {
    var currentFocus;
    /* Execute a function when someone writes in the text field. */
    symbol_input.addEventListener("input", function(e) {
        var a, b, val = this.value.toUpperCase();
        /* Close any already open lists of autocompleted values. */
        closeAllLists();
        if (!val) { return false;}
        currentFocus = -1;
        /* Create a DIV element that will contain the items (values). */
        a = document.createElement("DIV");
        a.setAttribute("id", this.id + "-autocomplete-list");
        a.setAttribute("class", "autocomplete-items");
        /* Append the DIV element as a child of the autocomplete container. */
        this.parentNode.appendChild(a);
        var node = symbol_tree;
        for (var char of val) {
            if (node.children[char] !== undefined) {
                node = node.children[char]
            } else {
                node = null;
                break;
            }
        }
        var arr = [];
        if (node !== null) {
            arr = node.symbols;
        }
        /* For each item in the array...*/
        for (var i = 0; i < arr.length; i++) {
            /* Create a DIV element for each matching element. */
            b = document.createElement("DIV");
            /* Make the matching letters bold:*/
            b.innerHTML = "<strong>" + arr[i].substr(0, val.length) + "</strong>";
            b.innerHTML += arr[i].substr(val.length);
            /* Insert a input field that will hold the current array item's value. */
            b.innerHTML += "<input type='hidden' value='" + arr[i] + "'>";
            /* Execute a function when someone clicks on the item value (DIV element). */
            b.addEventListener("click", function(e) {
                /* Insert the value for the autocomplete text field. */
                symbol_input.value = this.getElementsByTagName("input")[0].value;
                /* Close the list of autocompleted values, (or any other open lists of autocompleted values. */
                closeAllLists();
            });
            a.appendChild(b);
        }
    });
    /* Execute a function presses a key on the keyboard. */
    symbol_input.addEventListener("keydown", function(e) {
        var x = document.getElementById(this.id + "-autocomplete-list");
        if (x) {
            x = x.getElementsByTagName("div");
        } else {
            return;
        }
        if (e.keyCode == 40) {
            /* If the arrow DOWN key is pressed, increase the currentFocus variable.*/
            currentFocus++;
            /* And make the current item more visible. */
            addActive(x);
        } else if (e.keyCode == 38) { //up
            /* If the arrow UP key is pressed, decrease the currentFocus variable. */
            currentFocus--;
            /* And make the current item more visible:*/
            addActive(x);
        } else if (e.keyCode == 13) {
            /* If the ENTER key is pressed, prevent the form from being submitted. */
            e.preventDefault();
            if (currentFocus > -1) {
                /* And simulate a click on the "active" item. */
                x[currentFocus].click();
            }
        }
    });
    function addActive(x) {
        /* A function to classify an item as "active". */
        if (!x) return false;
        /* Start by removing the "active" class on all items. */
        removeActive(x);
        if (currentFocus >= x.length) currentFocus = 0;
        if (currentFocus < 0) currentFocus = (x.length - 1);
        /* Add class "autocomplete-active". */
        x[currentFocus].classList.add("autocomplete-active");
     }
    function removeActive(x) {
        /* A function to remove the "active" class from all autocomplete items. */
        for (var i = 0; i < x.length; i++) {
            x[i].classList.remove("autocomplete-active");
        }
    }

    function closeAllLists(elmnt) {
        /* Close all autocomplete lists in the document, except the one passed as an argument. */
        var x = document.getElementsByClassName("autocomplete-items");
        for (var i = 0; i < x.length; i++) {
            if (elmnt != x[i] && elmnt != symbol_input) {
                x[i].parentNode.removeChild(x[i]);
            }
        }
    }

    /* Execute a function when someone clicks in the document. */
    document.addEventListener("click", function (e) {closeAllLists(e.target);});
}

add_auto_complete(intraday_symbol_input);
add_auto_complete(daily_symbol_input);

// History and trade list live in sessionStorage: they survive a refresh but not a new tab or window.
const HISTORY_STORAGE_KEY = "charts.history";
const LAST_QUERY_STORAGE_KEY = "charts.last_query";

function read_session(key) {
    try {
        return JSON.parse(sessionStorage.getItem(key));
    } catch (e) {
        return null;
    }
}

function write_session(key, value) {
    try {
        sessionStorage.setItem(key, JSON.stringify(value));
    } catch (e) {}
}

function save_last_query(query) {
    write_session(LAST_QUERY_STORAGE_KEY, query);
}

function save_history() {
    write_session(HISTORY_STORAGE_KEY, Array.from(history_container.children).map(btn => ({
        symbol: btn.getAttribute("symbol"),
        date: btn.getAttribute("date"),
        entry: btn.getAttribute("entry"),
        exit: btn.getAttribute("exit"),
    })));
}

function is_valid_history_item(item) {
    return item !== null && typeof item === "object" && validateDate(String(item.date))
        && validateSymbol(String(item.symbol));
}

function add_history_button(item) {
    for (const existing of Array.from(history_container.children)) {
        if (existing.getAttribute("date") === item.date && existing.getAttribute("symbol") === item.symbol
            && existing.getAttribute("entry") === item.entry && existing.getAttribute("exit") === item.exit) {
            existing.remove();
        }
    }
    const btn = document.createElement("span");
    btn.className = "btn my-btn-outline my-btn-pill history-btn " + (isMobile ? "my-btn-outline-no-hover" : "my-btn-outline-hover");
    for (const key of ["symbol", "date", "entry", "exit"]) {
        btn.setAttribute(key, item[key]);
    }
    const icon = document.createElement("i");
    icon.className = "uil uil-history";
    const times = (item.entry || item.exit) ? ` ${item.entry || "?"}-${item.exit || "?"}` : "";
    btn.append(icon, ` ${item.date} ${item.symbol}${times}`);
    history_container.prepend(btn);
    history_card.classList.remove("hidden");
    save_history();
}

history_container.addEventListener("click", function(event) {
    const btn = event.target.closest(".history-btn");
    if (btn !== null) {
        fill_intraday_inputs(btn.getAttribute("symbol"), btn.getAttribute("date"),
                             btn.getAttribute("entry"), btn.getAttribute("exit"));
    }
});

// Forgets every past query except the chart on screen, which the next query still adds to the history.
document.getElementById("history-clear-btn").addEventListener("click", () => {
    history_container.innerHTML = "";
    history_card.classList.add("hidden");
    for (const arr of [historical_symbols, historical_dates, historical_entries, historical_exits]) {
        arr.splice(0, arr.length - 1);
    }
    try {
        sessionStorage.removeItem(HISTORY_STORAGE_KEY);
    } catch (e) {}
});

// Trade list: paste trades from the backtest summary and click one to chart it.
const TRADES_STORAGE_KEY = "charts.trades_text";
const trades_textarea = document.getElementById("trades-textarea");
const trades_input_container = document.getElementById("trades-input-container");
const trades_container = document.getElementById("trades-container");
const trades_hint = document.getElementById("trades-hint");
const trades_toggle_btn = document.getElementById("trades-toggle-btn");
var trades = [];

function parse_trades(text) {
    const parsed = [];
    const seen = new Set();
    // Columns can come in any order, so every token is recognized by its shape: yyyy-mm-dd is the date, the first
    // two hh:mm are the entry and exit times, long / short is the side, +1.2% is the gain, and the first other
    // plain word is the symbol. Prices and any other column are ignored.
    // Rows without their own date (e.g. the per-day tables in details.txt) inherit the date of the
    // closest preceding section header such as "== [ 2020-03-05 ] ====".
    let section_date = "";
    for (const line of text.split("\n")) {
        const tokens = line.split(/[|\s,]+/).filter(t => t.length > 0);
        const line_date = tokens.find(t => /^\d{4}-\d{2}-\d{2}$/.test(t));
        const times = tokens.filter(t => /^\d{1,2}:\d{2}(:\d{2})?$/.test(t));
        const symbol = tokens.find(t => /^[A-Za-z][A-Za-z0-9.\-]*$/.test(t) && !/^(long|short)$/i.test(t));
        if (symbol === undefined || times.length < 2) {
            // Not a trade. A line that holds a date starts a new section.
            if (line_date !== undefined) {
                section_date = line_date;
            }
            continue;
        }
        const date = line_date || section_date;
        if (!date) {
            continue;
        }
        const [entry, exit] = times.slice(0, 2).map(t => t.split(":").slice(0, 2).map(p => p.padStart(2, "0")).join(":"));
        const trade = {
            symbol: symbol.toUpperCase(),
            date: date,
            entry: entry,
            exit: exit,
            side: tokens.find(t => /^(long|short)$/i.test(t)) || "",
            gain: tokens.find(t => /^[+-]?\d+(\.\d+)?%$/.test(t)) || "",
        };
        const key = `${trade.symbol} ${trade.date} ${trade.entry} ${trade.exit}`;
        if (!seen.has(key)) {
            seen.add(key);
            parsed.push(trade);
        }
    }
    return parsed;
}

function render_trades() {
    trades_container.innerHTML = "";
    trades.forEach((trade, index) => {
        const btn = document.createElement("span");
        btn.className = "btn my-btn-outline my-btn-pill trade-btn " + (isMobile ? "my-btn-outline-no-hover" : "my-btn-outline-hover");
        btn.classList.add(trade.gain.startsWith("-") ? "trade-loss" : "trade-win");
        btn.dataset.index = index;
        btn.title = `${trade.side} ${trade.entry} - ${trade.exit}`.trim();
        btn.textContent = `${trade.symbol} ${trade.date} ${trade.entry}`;
        if (trade.gain) {
            const gain = document.createElement("span");
            gain.className = "trade-gain";
            gain.textContent = trade.gain;
            btn.appendChild(gain);
        }
        trades_container.appendChild(btn);
    });
}

function select_trade(index) {
    const trade = trades[index];
    fill_intraday_inputs(trade.symbol, trade.date, trade.entry, trade.exit);
}

function load_trades() {
    trades = parse_trades(trades_textarea.value);
    try {
        sessionStorage.setItem(TRADES_STORAGE_KEY, trades_textarea.value);
    } catch (e) {}
    render_trades();
    trades_hint.textContent = trades.length > 0 ? `${trades.length} trades loaded` : "No trades found";
    if (trades.length > 0) {
        trades_input_container.classList.add("hidden");
        trades_toggle_btn.innerHTML = "<i class='uil uil-edit'></i> EDIT";
    }
}

trades_container.addEventListener("click", function(event) {
    const btn = event.target.closest(".trade-btn");
    if (btn !== null) {
        select_trade(parseInt(btn.dataset.index));
    }
});
document.getElementById("trades-load-btn").addEventListener("click", load_trades);
document.getElementById("trades-clear-btn").addEventListener("click", () => {
    trades_textarea.value = "";
    load_trades();
    trades_input_container.classList.remove("hidden");
    trades_hint.textContent = "";
});
trades_toggle_btn.addEventListener("click", () => {
    trades_input_container.classList.toggle("hidden");
    trades_toggle_btn.innerHTML = trades_input_container.classList.contains("hidden")
        ? "<i class='uil uil-edit'></i> EDIT" : "<i class='uil uil-angle-up'></i> HIDE";
});

try {
    trades_textarea.value = sessionStorage.getItem(TRADES_STORAGE_KEY) || "";
} catch (e) {}
if (trades_textarea.value.trim().length > 0) {
    load_trades();
} else {
    trades_toggle_btn.innerHTML = "<i class='uil uil-angle-up'></i> HIDE";
}

// Restore history after a refresh. The last query is not in the history yet (it was the displayed chart),
// so add it too, unless the page just re-displayed it from the URL.
const saved_history = read_session(HISTORY_STORAGE_KEY);
if (Array.isArray(saved_history)) {
    for (const item of saved_history.reverse()) {
        if (is_valid_history_item(item)) {
            add_history_button({symbol: item.symbol, date: item.date, entry: item.entry || "", exit: item.exit || ""});
        }
    }
}
const saved_last = read_session(LAST_QUERY_STORAGE_KEY);
if (is_valid_history_item(saved_last)) {
    const n = historical_symbols.length - 1;
    const displayed = n >= 0 && historical_symbols[n] === saved_last.symbol && historical_dates[n] === saved_last.date
        && historical_entries[n] === (saved_last.entry || "") && historical_exits[n] === (saved_last.exit || "");
    if (!displayed) {
        add_history_button({symbol: saved_last.symbol, date: saved_last.date, entry: saved_last.entry || "", exit: saved_last.exit || ""});
    }
}
