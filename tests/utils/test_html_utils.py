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
    assert count_changed_lines([], ['a', 'b']) == (2, 0)
    assert count_changed_lines(['a', 'b'], []) == (0, 2)
    assert count_changed_lines(['a'], ['a']) == (0, 0)


def test_render_diff_page():
    template = (
        '<title>{{OUTPUT_NUM}}</title>{{BASE_COMMIT}} {{BASE_MESSAGE}} {{FILE_COUNT}} '
        '+{{TOTAL_ADDED}} -{{TOTAL_REMOVED}}{{INDEX}}{{FILES}}'
    )
    files = [
        DiffFile(path='pkg/a.py', table='<table>{{OUTPUT_NUM}}</table>', status='M', added=3, removed=1),
        DiffFile(path='pkg/b<x>.py', table='<table></table>', status='R', added=0, removed=0, old_path='pkg/old.py'),
    ]

    page = render_diff_page(
        template, files, output_num=7, logo_uri='file:///logo.png', base_commit='abc1234', base_message='fix <bug>'
    )

    assert '<title>7</title>abc1234 fix &lt;bug&gt; 2 files +3 -1' in page
    assert 'href="#file-0"' in page and 'id="file-1"' in page
    assert 'badge M' in page and 'badge R' in page
    assert 'pkg/b&lt;x&gt;.py' not in page  # directory and name are rendered separately
    assert 'b&lt;x&gt;.py' in page
    assert '&larr; pkg/old.py' in page
    # Code that looks like a placeholder is left alone
    assert '<table>{{OUTPUT_NUM}}</table>' in page
