import builtins
import difflib
import html as _html
import json
import keyword
import os
import re
import warnings
from dataclasses import dataclass

from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

warnings.filterwarnings('ignore', category=MarkupResemblesLocatorWarning)

regex_template = r'(?<![A-Za-z\-_\.\'">])({})(?![A-Za-z\-_=])'
keyword_names = set(keyword.kwlist)
keyword_pattern = re.compile(regex_template.format('|'.join(keyword_names)), re.VERBOSE)
builtin_names = [b for b in dir(builtins) if b.islower()] + ['self']
builtin_pattern = re.compile(regex_template.format('|'.join(builtin_names), re.VERBOSE))
method_pattern = re.compile(r'(def[\w <>\/]*?(\s|&nbsp;))(\w+)(\()', re.VERBOSE)
class_pattern = re.compile(r'(class[\w <>\/]*?(\s|&nbsp;))(\w+)([\:\(])', re.VERBOSE)
string_pattern = re.compile(r'(([\"\'])(?:(?=(\\?))\3.)*?\2)')
number_pattern = re.compile(r'(&nbsp;|\=|\(|\s|\-|\+|\*|\/)(\d+\.?\d*)')


def highlight_diff_table(diff_table: str) -> str:
    soup = BeautifulSoup(diff_table, 'html.parser')
    for td in soup.find_all('td', class_=lambda x: x is None):
        content = td.decode_contents()
        comment = ''
        if '#' in content:
            ind = content.find('#')
            content, comment = content[:ind], content[ind:]
            while content.count("'") % 2 == 1 or content.count('"') % 2 == 1:
                content, comment = content + comment, ''
                ind = content.find('#', ind + 1)
                if ind < 0:
                    break
                content, comment = content[:ind], content[ind:]
        if any(k in content for k in keyword.kwlist):
            content = keyword_pattern.sub(r'<span class="python_keyword">\1</span>', content)
        if any(k in content for k in builtin_names):
            content = builtin_pattern.sub(r'<span class="python_builtin">\1</span>', content)
        if 'def' in content:
            content = method_pattern.sub(r'\1<span class="python_method">\3</span>\4', content)
        if 'class' in content:
            content = class_pattern.sub(r'\1<span class="python_class">\3</span>\4', content)
        token = ''
        updated_content = ''
        i = 0
        while i < len(content):
            c = content[i]
            if (c == '<' and 'span' in content[i : i + 6]) or i == len(content) - 1:
                if i == len(content) - 1:
                    token += c
                token = string_pattern.sub(r'<span class="python_string">\1</span>', token)
                token = number_pattern.sub(r'\1<span class="python_number">\2</span>', token)
                updated_content += token
                token = ''
                if i < len(content) - 1:
                    updated_content += c
                i += 1
                while i < len(content) and c != '>':
                    c = content[i]
                    updated_content += c
                    i += 1
                if i < len(content):
                    c = content[i]
                    if c == '>':
                        updated_content += c
                    else:
                        token += c
            else:
                token += c
            i += 1
        if token:
            token = string_pattern.sub(r'<span class="python_string">\1</span>', token)
            token = number_pattern.sub(r'\1<span class="python_number">\2</span>', token)
            updated_content += token
        content = updated_content
        if comment:
            comment_span = '<span class="python_comment">'
            decorated_comment = comment_span
            for c in comment:
                if c == '<':
                    decorated_comment += '</span>'
                decorated_comment += c
                if c == '>':
                    decorated_comment += comment_span
            decorated_comment += '</span>'
            content += decorated_comment
        td.clear()
        content_soup = BeautifulSoup(content, 'html.parser')
        if str(content_soup):
            td.append(content_soup)
    return str(soup)


@dataclass(slots=True)
class DiffFile:
    """One changed file: its side-by-side diff table plus what the page header needs to describe it."""

    path: str
    table: str
    status: str  # git change type: M(odified), A(dded), D(eleted), R(enamed)
    added: int
    removed: int
    old_path: str | None = None  # only set when the file was renamed


def count_changed_lines(old_lines: list[str], new_lines: list[str]) -> tuple[int, int]:
    """Returns the number of (added, removed) lines between two versions of a file."""
    added = removed = 0
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False).get_opcodes():
        if tag in ('replace', 'delete'):
            removed += i2 - i1
        if tag in ('replace', 'insert'):
            added += j2 - j1
    return added, removed


def _format_path(file: DiffFile) -> str:
    directory, _, name = file.path.rpartition('/')
    res = ''
    if directory:
        res += f'<span class="dir">{_html.escape(directory)}/</span>'
    res += f'<span class="name">{_html.escape(name)}</span>'
    if file.old_path and file.old_path != file.path:
        res += f' <span class="from">&larr; {_html.escape(file.old_path)}</span>'
    return f'<span class="path">{res}</span>'


def _format_counts(file: DiffFile) -> str:
    """Counts plus GitHub-style five-block bar showing the added / removed proportion."""
    total = file.added + file.removed
    green = round(5 * file.added / total) if total else 0
    if file.added and not green:
        green = 1
    if file.removed and green == 5:
        green = 4
    red = 5 - green if total else 0
    blocks = ['a'] * green + ['d'] * red
    blocks += [''] * (5 - len(blocks))
    block_html = ''.join(f'<i class="{b}"></i>' for b in blocks)
    return (
        f'<span class="counts"><span class="add">+{file.added}</span> '
        f'<span class="del">&minus;{file.removed}</span> <span class="blocks">{block_html}</span></span>'
    )


def render_diff_page(
    template: str,
    files: list[DiffFile],
    output_num: str | int,
    logo_uri: str,
    base_commit: str,
    base_message: str,
) -> str:
    """Fills the diff page template with the given files.

    The template uses {{NAME}} placeholders. Everything is substituted in one pass, so code that happens to
    contain a placeholder is never expanded.
    """
    index = []
    sections = []
    for i, file in enumerate(files):
        file_id = f'file-{i}'
        header = f'<span class="badge {file.status}">{file.status}</span>{_format_path(file)}{_format_counts(file)}'
        index.append(f'<a href="#{file_id}">{header}</a>')
        sections.append(
            f'<details class="file card" id="{file_id}" open><summary>{header}</summary>'
            f'<div class="table-wrap">{file.table}</div></details>'
        )
    values = {
        'OUTPUT_NUM': _html.escape(str(output_num)),
        'LOGO_URI': _html.escape(logo_uri),
        'BASE_COMMIT': _html.escape(base_commit),
        'BASE_MESSAGE': _html.escape(base_message),
        'FILE_COUNT': f'{len(files)} file{"" if len(files) == 1 else "s"}',
        'TOTAL_ADDED': str(sum(f.added for f in files)),
        'TOTAL_REMOVED': str(sum(f.removed for f in files)),
        'INDEX': ''.join(index),
        'FILES': ''.join(sections),
    }
    return re.sub(r'\{\{(\w+)\}\}', lambda m: values.get(m.group(1), m.group(0)), template)


@dataclass(slots=True)
class ReportTable:
    """A titled table of the backtest report. Cells are plain text."""

    title: str
    rows: list[list[str]]
    headers: list[str] | None = None
    # Columns from this index on are right-aligned, as numbers are
    numeric_from: int | None = None
    # Adds a button that copies the rows as `| a | b |` lines
    copyable: bool = False
    # Consecutive tables with the same group share one card, titled with the group, and one copy button
    group: str | None = None


@dataclass(slots=True)
class ReportHighlight:
    """A headline number of the backtest report."""

    label: str
    value: str
    note: str | None = None


@dataclass(slots=True)
class ChartSeries:
    """One line of a report chart."""

    label: str
    values: list[float]
    color: str


@dataclass(slots=True)
class ReportChart:
    """A titled line chart of the backtest report, drawn in the browser."""

    title: str
    labels: list[str]
    series: list[ChartSeries]
    log_scale: bool = False


_SIGNED_NUMBER = re.compile(r'[+\-]\d')
_REPORT_TEMPLATE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'trade', 'html', 'report.html'
)


def _sign_class(text: str) -> str:
    """CSS class that colors a signed number such as +1.20% green and -0.50% red."""
    if _SIGNED_NUMBER.match(text):
        return 'pos' if text[0] == '+' else 'neg'
    return ''


def _format_table(table: ReportTable) -> str:
    def cell(tag: str, text: str, col: int) -> str:
        classes = []
        if table.numeric_from is not None and col >= table.numeric_from:
            classes.append('num')
        if tag == 'td' and _sign_class(text):
            classes.append(_sign_class(text))
        attr = f' class="{" ".join(classes)}"' if classes else ''
        return f'<{tag}{attr}>{_html.escape(text)}</{tag}>'

    head = ''
    if table.headers:
        head = '<thead><tr>' + ''.join(cell('th', h, i) for i, h in enumerate(table.headers)) + '</tr></thead>'
    body = ''.join('<tr>' + ''.join(cell('td', text, i) for i, text in enumerate(row)) + '</tr>' for row in table.rows)
    kv = '' if table.headers else ' class="kv"'
    return f'<div class="table-wrap"><table{kv}>{head}<tbody>{body}</tbody></table></div>'


def _format_tables(tables: list[ReportTable]) -> str:
    """Lays the tables out as cards, one per table or per group of tables."""
    cards = []
    i = 0
    while i < len(tables):
        members = [tables[i]]
        i += 1
        while tables[i - 1].group and i < len(tables) and tables[i].group == tables[i - 1].group:
            members.append(tables[i])
            i += 1
        first = members[0]
        title = first.group or first.title
        n_cols = max(len(row) for t in members for row in [t.headers or [], *t.rows])
        wide = ' wide' if n_cols > 4 else ''
        copy_button = ''
        if any(t.copyable for t in members):
            # A lone table is copied with its header row, the tables of a group are just their rows
            with_header = ' data-header' if not first.group and first.headers else ''
            copy_button = f'<button class="copy-btn" type="button"{with_header}>Copy</button>'
        if first.group:
            body = ''.join(f'<div class="sub-title">{_html.escape(t.title)}</div>{_format_table(t)}' for t in members)
        else:
            body = _format_table(first)
        cards.append(
            f'<section class="card{wide}"><div class="card-title"><span>{_html.escape(title)}</span>{copy_button}</div>'
            f'{body}</section>'
        )
    return ''.join(cards)


def render_report_page(
    output_num: str | int,
    logo_uri: str,
    subtitle: str,
    highlights: list[ReportHighlight],
    summary: list[ReportTable],
    profile: list[ReportTable],
    charts: list[ReportChart],
    diff_src: str | None = None,
) -> str:
    """Fills the backtest report template (trade/html/report.html). Tabs without content are left out.

    The template uses {{NAME}} placeholders that are substituted in one pass, like the diff page. `diff_src` is the
    address of the diff page (see render_diff_page) to show in the last tab, relative to the report.
    """
    panels = []
    if highlights or summary:
        kpis = ''.join(
            f'<div class="kpi"><div class="kpi-label">{_html.escape(h.label)}</div>'
            f'<div class="kpi-value {_sign_class(h.value)}">{_html.escape(h.value)}</div>'
            + (f'<div class="kpi-note">{_html.escape(h.note)}</div>' if h.note else '')
            + '</div>'
            for h in highlights
        )
        panels.append(('summary', 'Summary', f'<div class="kpis">{kpis}</div>' + _format_tables(summary)))
    if profile:
        panels.append(('profile', 'Profile', _format_tables(profile)))
    if charts:
        chart_html = ''
        for i, chart in enumerate(charts):
            spec = {
                'labels': chart.labels,
                'log': chart.log_scale,
                'series': [{'label': s.label, 'values': s.values, 'color': s.color} for s in chart.series],
            }
            # '<' is escaped so that the data can never end the script element
            data = json.dumps(spec).replace('<', '\\u003c')
            chart_html += (
                f'<section class="card chart"><div class="card-title">{_html.escape(chart.title)}</div>'
                f'<div class="chart-box"><canvas data-chart="chart-data-{i}"></canvas></div>'
                f'<script type="application/json" id="chart-data-{i}">{data}</script></section>'
            )
        panels.append(('charts', 'Charts', chart_html))
    if diff_src:
        # Loaded when the tab is first opened
        frame = f'<iframe title="Code diff" data-src="{_html.escape(diff_src)}"></iframe>'
        panels.append(('diff', 'Diff', frame))
    tabs = ''.join(
        f'<button class="tab" type="button" role="tab" id="tab-{name}" data-panel="{name}">{label}</button>'
        for name, label, _ in panels
    )
    panel_html = ''.join(
        f'<div class="panel {name}" role="tabpanel" id="panel-{name}" aria-labelledby="tab-{name}" hidden>{content}</div>'
        for name, _, content in panels
    )
    with open(_REPORT_TEMPLATE, 'r', encoding='utf-8') as f:
        template = f.read()
    values = {
        'OUTPUT_NUM': _html.escape(str(output_num)),
        'LOGO_URI': _html.escape(logo_uri),
        'SUBTITLE': _html.escape(subtitle),
        'TABS': tabs,
        'PANELS': panel_html,
    }
    return re.sub(r'\{\{(\w+)\}\}', lambda m: values.get(m.group(1), m.group(0)), template)
