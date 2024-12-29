import logging


def get_logger(name, handler = None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not handler:
        handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    logger.addHandler(handler)

    return logger