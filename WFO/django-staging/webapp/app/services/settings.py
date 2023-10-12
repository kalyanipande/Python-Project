import os
# from app.conf import ENVIRONMENT

STOMP_SERVER_HOST = str(os.getenv('AMQHost'))
STOMP_SERVER_PORT = str(os.getenv('AMQPort'))
STOMP_USE_SSL = False
STOMP_SERVER_USER = str(os.getenv('AMQUser'))
STOMP_SERVER_PASSWORD = str(os.getenv('AMQPassword'))
STOMP_CORRELATION_ID_REQUIRED = False
STOMP_TOPIC_NAME = "/queue/wfo_gui_notify"

# env = ENVIRONMENT['env']

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
         'WFOLogger': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }
}
