import os
import logging
from functools import wraps
from flask import request, jsonify, g

logger = logging.getLogger(__name__)

API_KEY_HEADER = 'X-API-Key'
API_KEY_QUERY = 'api_key'


def get_expected_api_key() -> str:
    return os.environ.get('API_KEY', '')


def is_auth_required() -> bool:
    return os.environ.get('REQUIRE_AUTH', 'false').lower() == 'true'


def check_api_key() -> bool:
    if not is_auth_required():
        return True

    expected_key = get_expected_api_key()
    if not expected_key:
        logger.warning('REQUIRE_AUTH is true but API_KEY is not set, skipping auth check')
        return True

    provided_key = request.headers.get(API_KEY_HEADER) or request.args.get(API_KEY_QUERY)

    if not provided_key:
        logger.warning('API key missing from request: %s %s', request.method, request.path)
        return False

    if provided_key != expected_key:
        logger.warning('Invalid API key provided for: %s %s', request.method, request.path)
        return False

    g.authenticated = True
    return True


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if is_auth_required() and not check_api_key():
            return jsonify({'error': 'Unauthorized', 'message': 'Valid API key required'}), 401
        return f(*args, **kwargs)
    return decorated


def init_app(app):
    @app.before_request
    def before_request_auth():
        if request.path in ('/', '/events') or request.path.startswith('/static/'):
            return

        if is_auth_required():
            if not check_api_key():
                return jsonify({'error': 'Unauthorized', 'message': 'Valid API key required'}), 401

    logger.info('Auth middleware initialized, auth required: %s', is_auth_required())
