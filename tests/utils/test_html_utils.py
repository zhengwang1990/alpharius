import textwrap

from alpharius.utils import (
    ChartSeries,
    DiffFile,
    ReportChart,
    ReportHighlight,
    ReportTable,
    count_changed_lines,
    highlight_diff_table,
    render_diff_page,
    render_report_page,
)


def test_highlight_diff_table():
    diff_table = textwrap.dedent("""
        <table>
        <tbody>
        <tr><td nowrap="nowrap"><span class="diff_add">import&nbsp;keyword</span></td></tr>
        <tr><td nowrap="nowrap"><span class="diff_add">import&nbsp;re</span></td></tr>
        <tr><td nowrap="nowrap"><span class="diff_add">def&nbsp;method(arg:&nbsp;str):</span></td></tr>
        <tr><td nowrap="nowrap"><span class="diff_add">&nbsp;&nbsp;&nbsp;&nbsp;'a string' # comment</span></td></tr>
        <tr><td nowrap="nowrap"><span class="diff_add">&nbsp;&nbsp;&nbsp;&nbsp;max([1, 2, 3])</span></td></tr>
        <tr><td nowrap="nowrap">class Abc:</td></tr>
        <tr><td nowrap="nowrap">&nbsp;&nbsp;&nbsp;&nbsp;pass</td></tr>
        <tr><td nowrap="nowrap">&nbsp;&nbsp;&nbsp;&nbsp;"another string"</td></tr>
        <tr><td nowrap="nowrap">&nbsp;&nbsp;&nbsp;&nbsp;assert x = 1.293</td></tr>
        <tr><td nowrap="nowrap">&nbsp;&nbsp;&nbsp;&nbsp;else:</td></tr>
        </tbody>
        </table>
        """)
    diff_table = highlight_diff_table(diff_table)
    assert 'python_keyword' in diff_table
    assert 'python_builtin' in diff_table
    assert 'python_class' in diff_table
    assert 'python_method' in diff_table
    assert 'python_string' in diff_table
    assert 'python_number' in diff_table
    assert '<span class="python_keyword">else</span>:' in diff_table


def test_count_changed_lines():
    assert count_changed_lines(['a', 'b', 'c'], ['a', 'x', 'c', 'd']) == (2, 1)


def test_render_diff_page():
    template = '{{OUTPUT_NUM}} {{BASE_MESSAGE}} {{FILE_COUNT}} +{{TOTAL_ADDED}}{{INDEX}}{{FILES}}'
    files = [DiffFile(path='pkg/a<b>.py', table='<table>{{OUTPUT_NUM}}</table>', status='M', added=3, removed=1)]

    page = render_diff_page(template, files, 7, 'file:///logo.png', 'abc1234', 'fix <bug>')

    assert page.startswith('7 fix &lt;bug&gt; 1 file +3')
    assert 'a&lt;b&gt;.py' in page  # paths are escaped
    assert '<table>{{OUTPUT_NUM}}</table>' in page  # code that looks like a placeholder is left alone


def test_render_report_page():
    summary = [
        ReportTable('Best <Trades>', [['QQQ', '+1.20%']], ['Symbol', 'Gain/Loss'], copyable=True, group='Trades'),
        ReportTable('Worst', [['SPY', '-0.50%']], ['Symbol', 'Gain/Loss'], copyable=True, group='Trades'),
        ReportTable('Stats', [['Beta', '1.1']], ['', 'SPY'], copyable=True),
    ]

    highlights = [ReportHighlight('Gain', '+1.20%'), ReportHighlight('Trades', '10', '2.00 per day')]

    page = render_report_page(3, 'file:///logo.png', 'a & b', highlights, summary, [], [])

    assert '{{' not in page.replace('{{NAME}}', '')  # every placeholder of the template is filled
    assert 'a &amp; b' in page and 'file:///logo.png' in page
    assert 'Best &lt;Trades&gt;' in page  # titles are escaped
    assert '<div class="kpi-note">2.00 per day</div>' in page
    # One card and one button for the group of trades, and a button of its own for the lone table
    assert page.count('<section class="card') == 2 and page.count('class="copy-btn"') == 2
    assert page.count('type="button" data-header>') == 1
    assert '<td class="pos">+1.20%</td>' in page and '<td class="neg">-0.50%</td>' in page
    # Tabs without content are left out
    assert 'id="tab-summary"' in page
    assert 'tab-profile' not in page and 'tab-charts' not in page and 'tab-diff' not in page

    charts = [ReportChart('2021 History', ['2021-01-04'], [ChartSeries('SPY </script>', [1.0], '#fff')])]
    page = render_report_page(3, 'file:///logo.png', '', [], [], [], charts, diff_src='diff.html?embed')
    assert 'id="tab-charts"' in page and 'id="tab-diff"' in page
    assert 'data-src="diff.html?embed"' in page
    assert '</script>' not in page.split('chart-data-0">')[1].split('</script>')[0]  # data cannot end its script
