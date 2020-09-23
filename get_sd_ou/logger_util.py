#%%
import logging

def init_logger(**kwargs):
    file_handler = kwargs.get('file_handler', 'sience_direct_logs.log')
    file_handler_level = kwargs.get('file_handler_level', logging.INFO)
    stream_handler = kwargs.get('stream_handler', True)
    stream_handler_level = kwargs.get('stream_handler_level', logging.DEBUG)
    format = kwargs.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    logger_level = kwargs.get('logger_level', logging.DEBUG)

    fh = logging.FileHandler('logs.log')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logging.basicConfig(format=format, handlers=[fh, ch])
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return logger
# %%
