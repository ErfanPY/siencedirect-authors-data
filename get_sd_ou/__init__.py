from logging.config import dictConfig

LOGGING_CONFIG = { 
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': { 
        'standard': { 
            'format': '%(asctime)s [%(levelname)s] [%(threadName)s:%(thread)d] : %(message)s',
        },
        'console_print': { 
            'format': '%(message)s'
        }
        ,
        'web_log': { 
            'format': '%(message)s\n------------------------------------\n'
        }
    },
    'handlers': { 
        'default': { 
            'level': 'DEBUG',
            'formatter': 'console_print',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        'file':{
            'level': 'INFO',
            'formatter': 'console_print',
            'class': 'logging.FileHandler',
            'filename': 'get_sd_ou/get_sd_ou.log',
        }
    },
    'loggers': { 
        '': {  # root logger
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': False
        },
        'mainLogger': { 
            'handlers': ['default', 'file'],
            'level': 'DEBUG',
            'propagate': False
        },
        '__main__': {  # if __name__ == '__main__'
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False
        },
    } 
}

dictConfig(LOGGING_CONFIG)
