import logging
import os
import re
import traceback

from flask import Flask, make_response, render_template

from . import scheduler, web


def handle_exception(e):
    cls = type(e)
    error_module = cls.__module__
    error_name = cls.__qualname__
    if error_module is not None and 'builtin' not in error_module:
        error_name = error_module + '.' + error_name
    pattern = r'([a-z]*api[a-z]*=)[a-zA-Z0-9]+'
    repl = r'\1<detached>'
    error_message = re.sub(pattern, repl, str(e))
    raw_tb_rows = traceback.format_exception(e)
    tb_rows = []
    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    keep = False
    for i, row in enumerate(raw_tb_rows):
        if 'File' in row:
            if base_dir in row:
                keep = True
            else:
                keep = False
        if keep and i != len(raw_tb_rows) - 1:
            tb_rows.append(row.replace(os.path.dirname(base_dir), '.'))
    tb = re.sub(pattern, repl, ''.join(tb_rows))
    resp = make_response(
        render_template('exception.html', error_name=error_name, error_message=error_message, traceback=tb)
    )
    resp.status_code = 500
    return resp


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    secret_key = os.environ.get('SECRET_KEY', 'dev')
    app.config.from_mapping(SECRET_KEY=secret_key)

    if test_config:
        app.config.from_mapping(test_config)

    app.logger.setLevel(logging.INFO)
    app.register_blueprint(web.bp)
    app.register_blueprint(scheduler.bp)
    if secret_key != 'dev':
        app.register_error_handler(Exception, handle_exception)

    return app
