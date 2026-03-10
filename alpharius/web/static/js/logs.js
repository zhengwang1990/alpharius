const elem = document.getElementById("datepicker");
function parseLocalDate(s) {
    const [y, m, d] = s.split('-').map(Number);
    return new Date(y, m - 1, d);
}
const min_date = parseLocalDate(DATES[0]);
const max_date = parseLocalDate(DATES[DATES.length - 1]);

const date_set = new Set(DATES.map(d => parseLocalDate(d).getTime()));

const dates_disabled = [];

for (let d = new Date(min_date); d <= max_date; d.setDate(d.getDate() + 1)) {
    if (!date_set.has(d.getTime())) {
        dates_disabled.push(new Date(d));
    }
}
const datepicker = new Datepicker(elem, {
    autohide: true,
    format: "yyyy-mm-dd",
    minDate: min_date,
    maxDate: max_date,
    datesDisabled: dates_disabled,
});
datepicker.setDate(parseLocalDate(CURRENT_DATE));
elem.addEventListener("changeDate", function(event){
    location.href = "logs?date=" + datepicker.getDate("yyyy-mm-dd");
});

const logger_select = document.getElementById("logger-select");
logger_select.addEventListener("change", function(event){
    var logger = event.target.value;
    document.getElementById("log-" + current_logger).style.display = "none";
    document.getElementById("log-" + logger).style.removeProperty("display");
    if ((current_logger === "Trading") && (level_select.value !== "debug")) {
        level_select.value = "debug";
        update_log_level("debug");
    } else if ((current_logger !== "Trading") && (logger == "Trading") && (level_select.value === "debug")) {
        level_select.value = "info";
        update_log_level("info");
    }
    current_logger = logger;
});

const level_select = document.getElementById("level-select");
level_select.addEventListener("change", function(event){
    var level = event.target.value;
    update_log_level(level);
});

function update_log_level(level) {
    visible = ["debug", "info", "warning", "error"];
    invisible = [];
    if (level === "info") {
        visible = ["info", "warning", "error"];
        invisible = ["debug"];
    }
    if (level === "warning") {
        visible = ["warning", "error"];
        invisible = ["debug", "info"];
    }
    if (level === "error") {
        visible = ["error"];
        invisible = ["debug", "info", "warning"];
    }
    for (const cls of visible) {
        elements = document.getElementsByClassName("log-entry-" + cls);
        for (const elem of elements) {
            elem.style.removeProperty("display");
        }
    }
    for (const cls of invisible) {
        elements = document.getElementsByClassName("log-entry-" + cls);
        for (const elem of elements) {
            elem.style.display = "none";
        }
    }
}

// Back-to-top button
let btt_button = document.getElementById("btn-back-to-top");
let gtb_button = document.getElementById("btn-go-to-bottom");
btt_button.addEventListener("click", backToTop);
gtb_button.addEventListener("click", goToBottom);
function backToTop() {
    document.documentElement.scrollTop = 0;
}
function goToBottom() {
    document.documentElement.scrollTop = document.documentElement.scrollHeight - window.innerHeight;
}
