const intraday_datepicker = new Datepicker(document.getElementById("intraday-datepicker"), {
    autohide: true,
    format: "M dd yyyy",
    maxDate: new Date(),
    daysOfWeekDisabled: [0, 6]
});

const daily_datepicker = new DateRangePicker(document.getElementById("daily-datepicker"), {
    autohide: true,
    format: "M dd yyyy",
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
var symbol_tree = {symbols: [], children: {}}
const symbol_set = new Set(ALL_SYMBOLS)
const intraday_alert = document.getElementById("intraday-alert");
const intraday_symbol_input = document.getElementById("intraday-symbol-input");
const intraday_chart_container = document.getElementById("intraday-chart-container");
const intraday_chart_name = document.getElementById("intraday-chart-name");
const intraday_button = document.getElementById("intraday-chart-btn");
const daily_alert = document.getElementById("daily-alert");
const daily_symbol_input = document.getElementById("daily-symbol-input");
const daily_chart_container = document.getElementById("daily-chart-container");
const daily_chart_name = document.getElementById("daily-chart-name");
const daily_button = document.getElementById("daily-chart-btn");

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

function get_chart_data(dates, symbol, timeframe) {
    var xmlHttp = new XMLHttpRequest();
    var route;
    if (timeframe === "intraday") {
        route = `/charts_data?date=${dates[0]}&symbol=${symbol}&timeframe=intraday`
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
    get_chart_data(dates, symbol, timeframe);
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
    const chart_config = {
        type: "bar",
        data: data,
        options: {
            maintainAspectRatio: false,
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
        plugins: [candlestick, barPosition, crosshair]
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
        intraday_button.innerHTML = "OK";
    } else {
        button_state = 0;
        intraday_button.innerHTML = "GO";
    }
    intraday_button.classList.toggle("btn-secondary");
    intraday_button.classList.toggle("btn-outline-secondary");
}
intraday_symbol_input.addEventListener("input", () => {
    if (button_state === 1) {
        toggle_button_state();
    }
});
intraday_datepicker.element.addEventListener("changeDate", () => {
    if (button_state === 1) {
        toggle_button_state();
    }
});

intraday_button.addEventListener("click", () => {
    if (button_state === 0) {
        if (get_chart("intraday") === 0) {
            toggle_button_state();
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
    get_chart_data([INIT_DATE], INIT_SYMBOL, "intraday");
    update_chart("intraday");
} else {
    displayAlert("info", "Enter date and symbol. Then click GO.", "intraday");
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
    displayAlert("info", "Enter start date, end date and symbol. Then click GO.", "daily");
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

function convert_date_input() {
    const inputVal = this.value;
    const yyyy_mm_dd_regex = /^\d{4}-\d{2}-\d{2}$/;
    // Check if the input is in the yyyy-mm-dd format
    if (yyyy_mm_dd_regex.test(inputVal)) {
        const parts = inputVal.split('-');
        const year = parts[0];
        const month = parseInt(parts[1]) - 1; // Months are 0-indexed (0=Jan, 11=Dec)
        const day = parts[2];

        // Define the month names
        const monthNames = [
          'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
        ];
        const outputMonth = monthNames[month];
        this.value = `${outputMonth} ${day} ${year}`;
    }
}

document.getElementById("intraday-datepicker").addEventListener("input", convert_date_input);
document.getElementById("interday-start-input").addEventListener("input", convert_date_input);
document.getElementById("interday-end-input").addEventListener("input", convert_date_input);
