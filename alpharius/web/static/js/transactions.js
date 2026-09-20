const processor_select = document.getElementById("processor-select");
const start_input = document.getElementById("start-input");
const end_input = document.getElementById("end-input");

function parseLocalDate(s) {
    const [y, m, d] = s.split("-").map(Number);
    return new Date(y, m - 1, d);
}

// Same rules as the charts page: trading days only, nothing in the future.
// No range picked means all transactions, which the x inside the box goes back to.
const range_picker = new DateRangePicker(document.getElementById("date-range"), {
    autohide: true,
    format: "yyyy-mm-dd",
    maxDate: new Date(),
    daysOfWeekDisabled: [0, 6],
});
if (ACTIVE_START && ACTIVE_END) {
    range_picker.setDates(parseLocalDate(ACTIVE_START), parseLocalDate(ACTIVE_END));
}

// Reload with whichever filters are set.
function reload(start, end) {
    const params = new URLSearchParams();
    if (processor_select.value !== "ALL PROCESSORS") {
        params.set("processor", processor_select.value);
    }
    if (start && end) {
        params.set("start_date", start);
        params.set("end_date", end);
    }
    const query = params.toString();
    window.location.href = "transactions" + (query ? "?" + query : "");
}

function get_range() {
    return range_picker.getDates("yyyy-mm-dd");
}

processor_select.addEventListener("change", () => reload(...get_range()));

// Picking one end fills in the other with the same day, so a lone pick shows that single day.
// A start moved past the end drags the end along, like the picker's own end date limit does.
for (const input of [start_input, end_input]) {
    input.addEventListener("changeDate", () => {
        let [start, end] = get_range();
        if (!start && !end) {
            return;
        }
        start = start || end;
        end = end || start;
        if (start > end) {
            if (input === start_input) {
                end = start;
            } else {
                start = end;
            }
        }
        reload(start, end);
    });
}

const date_clear = document.getElementById("date-clear");
if (date_clear) {
    date_clear.addEventListener("click", () => reload());
}

// Below 1500px the processor, dates and prices are dropped from the table (see base.css / web.py),
// so tapping a row pops up a tooltip with them instead.
init_row_tip(row => [
    ["Processor", row.dataset.processor || "UNKNOWN"],
    ["Entry", row.dataset.entry],
    ["Exit", row.dataset.exit],
]);
