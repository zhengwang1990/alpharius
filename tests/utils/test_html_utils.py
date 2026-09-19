import textwrap

from alpharius.utils import DiffFile, count_changed_lines, highlight_diff_table, render_diff_page


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


def test_count_changed_lines():
    assert count_changed_lines(['a', 'b', 'c'], ['a', 'x', 'c', 'd']) == (2, 1)


def test_render_diff_page():
    template = '{{OUTPUT_NUM}} {{BASE_MESSAGE}} {{FILE_COUNT}} +{{TOTAL_ADDED}}{{INDEX}}{{FILES}}'
    files = [DiffFile(path='pkg/a<b>.py', table='<table>{{OUTPUT_NUM}}</table>', status='M', added=3, removed=1)]

    page = render_diff_page(template, files, 7, 'file:///logo.png', 'abc1234', 'fix <bug>')

    assert page.startswith('7 fix &lt;bug&gt; 1 file +3')
    assert 'a&lt;b&gt;.py' in page  # paths are escaped
    assert '<table>{{OUTPUT_NUM}}</table>' in page  # code that looks like a placeholder is left alone
