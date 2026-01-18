# Superset configuration for JobInsight
import os

# Basic config
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'jobinsight_superset_secret_key_12345')
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Cache config
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Webserver config
WEBSERVER_TIMEOUT = 300
SUPERSET_WEBSERVER_PORT = 8088

# Enable CORS for local development
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Disable CSRF for easier local development
WTF_CSRF_ENABLED = False

# Default language
BABEL_DEFAULT_LOCALE = 'en'

# Row limit
ROW_LIMIT = 10000
SQL_MAX_ROW = 100000
