import textwrap

from alpharius.utils import highlight_diff_table


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
    assert "python_keyword" in diff_table
    assert "python_builtin" in diff_table
    assert "python_class" in diff_table
    assert "python_method" in diff_table
    assert "python_string" in diff_table
    assert "python_number" in diff_table
