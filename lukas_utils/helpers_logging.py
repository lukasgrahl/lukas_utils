import logging
import os
import sys


def _get_log_path():
    if "PATH_LOG_DIR" not in os.environ.keys():
        raise KeyError("please add PATH_LOG_DIR to os.environ")

    log_dir_path = os.environ["PATH_LOG_DIR"]
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)

    return log_dir_path


def get_logger(name, file_level: int = 10, stream_level: int = 20):
    """
    Gets custom logger
    :param name: file name, ideally this should be os.path.basename(__file__)
    :param file_level: set logging level for .log file: 0 = None, 10 = Debug, 20 = Info, 30 = Warning, 40 = Error
    :param stream_level: set logging level for .log file: 0 = None, 10 = Debug, 20 = Info, 30 = Warning, 40 = Error
    :return:
    """

    log_dir_path = _get_log_path()

    if "." in name:
        name = name.split(".")[0]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    file_log = os.path.join(log_dir_path, f"{name}.log")

    fhandler = logging.FileHandler(file_log, "w+")  # log file handler
    fhandler.setLevel(file_level)

    shandler = logging.StreamHandler(stream=sys.stdout)
    shandler.setLevel(stream_level)

    format_f = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s : %(message)s")
    format_s = logging.Formatter("%(name)s | %(levelname)s : %(message)s")

    fhandler.setFormatter(format_f)
    shandler.setFormatter(format_s)

    logger.addHandler(fhandler)
    logger.addHandler(shandler)
    return logger
