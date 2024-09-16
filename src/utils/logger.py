import logging

def setup_logger():
    """
    Set up and return a logger instance.
    """
    logger = logging.getLogger('ScanService')
    logger.setLevel(logging.DEBUG)

    # Create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger

def get_logger(name):
    """
    Get a logger instance with the given name.
    """
    return logging.getLogger(f'ScanService.{name}')
