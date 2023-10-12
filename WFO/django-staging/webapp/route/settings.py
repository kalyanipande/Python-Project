import os
from decouple import config
import logging

global logger
logger = logging.getLogger("WFOLogger")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'b6b!o-wg9m#o01id^*bl4fy*5qr#4oj@u12$(6nw1z(g92s03r'

# SECURITY WARNING: don't run with debug turned on in production!
namespaces = os.getenv('NAMESPACE', '')
DEBUG = True
if 'test' in namespaces or 'staging' in namespaces:
    DEBUG = True

ALLOWED_HOSTS = ["*"]
# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'django_filters',
    'corsheaders',
    'drf_multiple_model',
    'django_prometheus',
    'adf_proxy',
    'app',
    'uam.apps.UamConfig',
    'django_apscheduler',
    'drf_yasg',
]

CONFIG_SETTINGS = {
    "IS_SSO_ENABLED": False,
    "IS_LOCAL_DATABASE_CONNECT": False,
    # Mandatory True [Local DB & All other connection], False [Live DB & All other Connection]
    "IS_STATIC_TOKEN_FOR_LOCAL_USAGE": True  # bool(os.environ.get("ENV_IS_SSO_ENABLED", True))
    # Mandatory True [Static Token and User Flow], False [Live Token and User Flow]
}

MIDDLEWARE = [
    'django_prometheus.middleware.PrometheusBeforeMiddleware',  # Mandatory
    'corsheaders.middleware.CorsMiddleware',  # Mandatory
    'django.middleware.security.SecurityMiddleware',  # Mandatory
    'django.contrib.sessions.middleware.SessionMiddleware',  # Mandatory
    'django.middleware.common.CommonMiddleware',  # Mandatory
    # 'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',  # Mandatory
    'django.contrib.messages.middleware.MessageMiddleware',  # Mandatory
    'django.middleware.clickjacking.XFrameOptionsMiddleware',  # Mandatory
    'django_prometheus.middleware.PrometheusAfterMiddleware',  # Mandatory
    'adf_proxy.middleware.ProxyAuthTokenCheckMiddleware',  # Mandatory
    'app.middleware.ApplicationMiddlewares'  # Mandatory
]

if bool(os.environ.get("IS_GATEKEEPER_ENABLED")):
    MIDDLEWARE.append('adf_proxy.middleware.ProxyAuthTokenCheckMiddleware')
    MIDDLEWARE.append('app.middleware.ApplicationMiddlewares')

CORS_ORIGIN_ALLOW_ALL = True
CORS_ALLOW_CREDENTIALS = False
ROOT_URLCONF = 'route.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'app', 'static')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'route.wsgi.application'

# SESSION_COOKIE_AGE = 360 # five minites in seconds
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_EXPIRE_AT_BROWSER_CLOSE = True
X_FRAME_OPTIONS = "DENY"  # DENY

SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

if not CONFIG_SETTINGS["IS_LOCAL_DATABASE_CONNECT"]:
    PG_HOST = str(os.environ.get("PG_HOST"))
    PG_PORT = 5432
    PG_USER = str(os.environ.get("PG_USERNAME"))
    PG_PASS = str(os.environ.get("PG_PASSWORD"))
    PG_DB_NAME = "postgres"
else:
    PG_HOST = "production.aibi-prod-fp.ch-dc-os-gsn-107.k8s.dyn.nesc.nokia.net"
    PG_PORT = 30749
    PG_USER = "postgres"
    PG_PASS = "tJEhoVyggE58DYyD"
    PG_DB_NAME = "postgres"

if CONFIG_SETTINGS["IS_STATIC_TOKEN_FOR_LOCAL_USAGE"]:
    CORS_ALLOW_HEADERS = [
        'accept',
        'accept-encoding',
        'authorization',
        'content-type',
        'dnt',
        'origin',
        'user-agent',
        'x-csrftoken',
        'x-requested-with',
        'X-KC-Token',
        'X-Proxy-Claims',
        'X-Token-Status'
    ]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': PG_DB_NAME,
        'USER': PG_USER,
        'PASSWORD': PG_PASS,
        'HOST': PG_HOST,
        'PORT': PG_PORT,
    },
    'FIR_SQL':{
            'ENGINE': 'mssql',
            'NAME': 'insights-azsql',
            'USER': 'graphql_user',
            'PASSWORD': 'Welcome12345',
            'HOST': 'tcp:insightscare-sqlserver.database.windows.net',
            'PORT': '1433',

            'OPTIONS': {
                'driver': 'ODBC Driver 18 for SQL Server',
            },
        }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.AllowAny',
    )
}

# to handle multiple database , one for User mgmt and other for Application level data
# DATABASE_ROUTERS = ['db_router.AuthRouter']

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s.%(msecs)03d | %(levelname)s | %(filename)s | %(funcName)s | %(lineno)d | %(message)s'

        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'generic': {
            'format': '%(asctime)s [%(process)d] [%(levelname)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'class': 'logging.Formatter',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'django.request': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'django.server': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
'django.db.backends': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'webapp': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True
        }
        , 'WFOLogger': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }
}

CACHES = {
    'default': {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://wfo-staging-redis:6379",
        'OPTIONS': {
            'REDIS_CLIENT_CLASS': 'rediscluster.RedisCluster',
            'CONNECTION_POOL_CLASS': 'rediscluster.connection.ClusterConnectionPool',
            'CONNECTION_POOL_KWARGS': {
                'skip_full_coverage_check': True  # AWS ElasticCache has disabled CONFIG commands
            }
        }
        , "KEY_PREFIX": "smia",

    }
}

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

STATIC_URL = '/stattic/'

STATIC_ROOT = os.path.join(BASE_DIR, 'app/static')

STOMP_SERVER_HOST = str(os.getenv('AMQHost'))
STOMP_SERVER_PORT = str(os.getenv('AMQPort'))
STOMP_USE_SSL = False
STOMP_SERVER_USER = str(os.getenv('AMQUser'))
STOMP_SERVER_PASSWORD = str(os.getenv('AMQPassword'))
STOMP_CORRELATION_ID_REQUIRED = False
STOMP_TOPIC_NAME = "/queue/wfo_gui_notify"
ELASTIC_SIMILAR_INDEX = "wfo_similar_cases"
ELASTIC_RECOMMEND_INDEX = "wfo_recommendations"
ELASTIC_RELEVANT_INDEX = ["wfo_similar_cases", "wfo_relevant_cases"]
QUERY_AGGREGATOR_URL = str(os.getenv('QAURL'))
# QUERY_AGGREGATOR_URL = "localhost"
QUERY_AGGREGATOR_PORT = str(os.getenv('QAPORT'))
# QUERY_AGGREGATOR_PORT = "5000"
ES_PASSWORD = str(os.getenv("ES_PASSWORD"))

# FIR Dataverse config
OAUTHTOKEN_ENDPOINT = 'https://login.microsoftonline.com/5d471751-9675-428d-917b-70f44f9630b0/' \
                      'oauth2/v2.0/token'
CLIENT_ID = '2839c028-5b25-40d1-b315-db9e0e9043d5'
CLIENT_SECRET = 'i4Vpt~5u8qYI-yAPw4-1ZWjK4I~GfH3S40'
SCOPE = 'https://org6f6ccb85.crm4.dynamics.com/.default'
API_URL = "https://org6f6ccb85.crm4.dynamics.com/api/data/v9.1/" \
          "crbdd_fir_case_summary_detailses/"

# CCR Prod Dataverse config
CCR_OAUTHTOKEN_ENDPOINT = 'https://login.microsoftonline.com/5d471751-9675-428d-917b-70f44f9630b0/oauth2/v2.0/token'
CCR_CLIENT_ID = 'adee3f3f-a4c2-45c6-a30b-6bc288c151d0'
CCR_CLIENT_SECRET = '7sb7Q~~jdVsA_7tvijlJO8oVw23o5ZpuWTsnK'
CCR_SCOPE = 'https://org6f6ccb85.crm4.dynamics.com/.default'
CCR_API_URL = "https://org6f6ccb85.crm4.dynamics.com/api/data/v9.1/crbdd_ccr_wfo_casesummaries/"

if not CONFIG_SETTINGS["IS_LOCAL_DATABASE_CONNECT"]:
    print("Data center name is:", str(os.getenv('DATA_CENTER')))
    if 'americas' in str(os.getenv('DATA_CENTER')):
        print("Americas Bucket Details")
        BUCKET = "wfo-test-validation"
        ACCESS_KEY = "6TQ7MYACFHR64LHDUTNY"
        S3_SECRET_KEY = "UYPyyd5CgPotmJBZ7qmInvvGfZjTaPHnf9uYs6pF"
        ENDPOINT = "https://ch-dc-s3-gsn-33.eecloud.nsn-net.net"
        POST_CASE_URL = "https://test-validation-de-staging.americas.abi.dyn.nesc.nokia.net/submit"
        POST_LEXICON_URL = "https://test-validation-de-staging.americas.abi.dyn.nesc.nokia.net/submit_scheduleAPI"
        ES_ENDPOINT = 'elastic-es-http'
        ES_USERNAME = 'elastic'
        ES_PORT = 9200
        # This is for Digimops Integratons
        WFO_DATA_URL = 'https://nokiawfoqa.ext.net.nokia.com/#/pages/table/case-recomendation/'
    elif 'wfo-global-services-care-production' in str(os.getenv('DATA_CENTER')):
        print("Azure Bucket Details")
        BUCKET = "wfo-global-services-care"
        ACCESS_KEY = "6TQ7MYACFHR64LHDUTNY"
        S3_SECRET_KEY = "UYPyyd5CgPotmJBZ7qmInvvGfZjTaPHnf9uYs6pF"
        ENDPOINT = "https://ch-dc-s3-gsn-33.eecloud.nsn-net.net"
        POST_CASE_URL = "https://de-global-services-care-production.wfo.americas.azaide.dyn.nesc.nokia.net/submit"
        POST_LEXICON_URL =  "https://de-global-services-care-production.wfo.americas.azaide.dyn.nesc.nokia.net/submit_scheduleAPI"
        ES_ENDPOINT = 'elastic-es-http'
        ES_USERNAME = 'elastic'
        ES_PORT = 9200
        # This is for Digimops Integratons
        WFO_DATA_URL = 'https://nokiawfo.ext.net.nokia.com/#/pages/table/case-recomendation/'
    else:
        print("Asia Bucket Details")
        BUCKET = "wfo-global-services-care"
        ACCESS_KEY = "46849BVQEHLVWZY01NXT"
        S3_SECRET_KEY = "JgMQZAds8iH4jcAcT84ilGHWqRPeT1dOtQSAyEwl"
        ENDPOINT = "https://bh-dc-s3-dhn-15.eecloud.nsn-net.net"
        POST_CASE_URL = "https://de-wfo-global-services-care-staging.asia.abi.dyn.nesc.nokia.net/submit"
        POST_LEXICON_URL = "https://de-wfo-global-services-care-staging.asia.abi.dyn.nesc.nokia.net/submit_scheduleAPI"
        ES_ENDPOINT = 'elastic-es-http'
        ES_USERNAME = 'elastic'
        ES_PORT = 9200
        # This is for Digimops Integratons
        WFO_DATA_URL = 'https://nokiawfoqa.ext.net.nokia.com/#/pages/table/case-recomendation/'
else:
    print("china Bucket Details")
    BUCKET = "wfo-test-validation"
    ACCESS_KEY = "6TQ7MYACFHR64LHDUTNY"
    S3_SECRET_KEY = "UYPyyd5CgPotmJBZ7qmInvvGfZjTaPHnf9uYs6pF"
    ENDPOINT = "https://ch-dc-s3-gsn-33.eecloud.nsn-net.net"
    POST_CASE_URL = "https://test-validation-de-staging.americas.abi.dyn.nesc.nokia.net/submit"
    POST_LEXICON_URL = "https://test-validation-de-staging.americas.abi.dyn.nesc.nokia.net/submit_scheduleAPI"

# S3_CONFIG_DATA_PATH = "development" + "/config_data"
# S3_SIMILAR_CASES_PATH = "development" + "/similar_cases"
# RECOMMENDATIONS_PATH = "development" + "/recommendations"

S3_CONFIG_DATA_PATH = str(os.getenv('CONFIG_DATA_PATH'))
S3_SIMILAR_CASES_PATH = str(os.getenv('SIMILARITY_PATH'))
RECOMMENDATIONS_PATH = str(os.getenv('RECOMMENDATIONS_PATH'))
