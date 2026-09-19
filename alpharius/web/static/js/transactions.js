const processor_select = document.getElementById("processor-select");
const date_input = document.getElementById("date-input");

function parseLocalDate(s) {
    const [y, m, d] = s.split("-").map(Number);
    return new Date(y, m - 1, d);
}

// Same rules as the charts page: trading days only, nothing in the future.
// No date picked means all transactions, so the picker can be cleared from its footer.
const datepicker = new Datepicker(date_input, {
    autohide: true,
    clearButton: true,
    format: "yyyy-mm-dd",
    maxDate: new Date(),
    daysOfWeekDisabled: [0, 6],
});
if (ACTIVE_DATE) {
    datepicker.setDate(parseLocalDate(ACTIVE_DATE));
}

// Reload with whichever filters are set.
function reload() {
    const params = new URLSearchParams();
    if (processor_select.value !== "ALL PROCESSORS") {
        params.set("processor", processor_select.value);
    }
    const date = datepicker.getDate("yyyy-mm-dd");
    if (date) {
        params.set("date", date);
    }
    const query = params.toString();
    window.location.href = "transactions" + (query ? "?" + query : "");
}

processor_select.addEventListener("change", reload);
date_input.addEventListener("changeDate", reload);

// The x inside the box is a shortcut for the picker's Clear button. It only exists while a date is set.
const date_clear = document.getElementById("date-clear");
if (date_clear) {
    date_clear.addEventListener("click", () => datepicker.setDate({clear: true}));
}

// Below 1500px the processor, dates and prices are dropped from the table (see base.css / web.py),
// so tapping a row pops up a tooltip with them instead.
init_row_tip(row => [
    ["Processor", row.dataset.processor || "UNKNOWN"],
    ["Entry", row.dataset.entry],
    ["Exit", row.dataset.exit],
]);
