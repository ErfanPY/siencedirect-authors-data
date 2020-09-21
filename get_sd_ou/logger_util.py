#%%
import logging

def init_logger(**kwargs):
    file_handler = kwargs.get('file_handler', default='sience_direct_logs.log')
    file_handler_level = kwargs.get('file_handler_level', default=logging.INFO)
    stream_handler = kwargs.get('stream_handler', default=True)
    stream_handler_level = kwargs.get('stream_handler_level', default=logging.DEBUG)
    format = kwargs.get('format', default='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_level = kwargs.get('logger_level', default=logging.DEBUG)

    fh = logging.FileHandler('logs.log')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logging.basicConfig(format=format, handlers=[fh, ch])
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    return logger
# %%
