import builtins
import keyword
import re
import warnings

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
number_pattern = re.compile(r'(&nbsp;|\=|\(|\s)(\d+\.?\d*)')


def highlight_diff_table(diff_table: str) -> str:
    soup = BeautifulSoup(diff_table, 'html.parser')
    for td in soup.find_all('td', class_=lambda x: x is None):
        content = td.decode_contents()
        comment = ''
        if '#' in content:
            ind = content.find('#')
            content, comment = content[:ind], content[ind:]
            while sum(c == "'" for c in content) % 2 == 1 or sum(c == '"' for c in content) % 2 == 1:
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
            if (c == '<' and 'span' in content[i:i + 6]) or i == len(content) - 1:
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
