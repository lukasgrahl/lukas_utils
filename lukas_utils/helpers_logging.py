import logging
import os
import sys


def _get_environ_vars():
    if "PATH_LOG_DIR" not in os.environ.keys():
        raise KeyError("please add PATH_LOG_DIR to os.environ")

    is_not_log_stream, is_not_log_file = False, False
    if "LEVEL_LOG_STREAM" not in os.environ.keys():
        is_not_log_stream = True
        raise KeyError(
            "please add LEVEL_LOG_STREAM to os.environ, otherwise defaulting to 20"
        )

    if "LEVEL_LOG_FILE" not in os.environ.keys():
        is_not_log_file = True
        raise KeyError(
            "please add LEVEL_LOG_FILE to os.environ, otherwise defaulting to 10"
        )

    log_dir_path = os.environ["PATH_LOG_DIR"]

    if is_not_log_stream:
        level_log_stream = 20
    else:
        level_log_stream = os.environ["LEVEL_LOG_STREAM"]

    if is_not_log_file:
        level_log_file = 10
    else:
        level_log_file = os.environ["LEVEL_LOG_FILE"]

    # makedir if needed
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)

    return log_dir_path, level_log_stream, level_log_file


def get_logger(name, file_level: int = None, stream_level: int = None):
    """
    Gets custom logger
    :param name: file name, ideally this should be os.path.basename(__file__)
    :param file_level: set logging level for .log file: 0 = None, 10 = Debug, 20 = Info, 30 = Warning, 40 = Error
    :param stream_level: set logging level for .log file: 0 = None, 10 = Debug, 20 = Info, 30 = Warning, 40 = Error
    :return:
    """

    log_dir_path, level_log_stream, level_log_file = _get_environ_vars()

    if file_level is None:
        file_level = level_log_file

    if stream_level is None:
        stream_level = level_log_stream

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
