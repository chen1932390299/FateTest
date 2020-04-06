# coding=utf-8
import logging
import os
import sys
import logging.handlers
import re


def logger(schedule_name, console_print=True, logging_level=None, console_debug_level=logging.INFO):
    """
    :param schedule_name: the log scheduler  name
    :param console_print: if True console print log ,False not .
    :param logging_level: file logging generate levels support: ["DEBUG","INFO","WARNING","ERROR"].
    :param console_debug_level: it can be str also ,for exp: console_debug_level="INFO",default set logging.INFO.
    :return: logging handlers
    """
    log_fmt = "[%(levelname)s]%(asctime)s line %(lineno)d :\n%(message)s"
    c_fmt = "[%(levelname)s]%(asctime)s %(filename)s.%(funcName)s():line %(lineno)d :\n%(message)s"
    date_format = "%Y-%m-%d %H:%M:%S %a"
    # setting console output log level
    logging.basicConfig(level=console_debug_level,
                        format=c_fmt,
                        datefmt=date_format,

                        )
    levels = []
    if isinstance(logging_level, list):
        for level in logging_level:
            if level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
                levels.append(level)
        if levels:
            stamp = f"{schedule_name}.log"
            logsdir = os.path.join(os.getcwd(), "demo_logs")
            if os.path.exists(logsdir):
                for p in levels:
                    if os.path.exists(os.path.join(logsdir, p)):
                        pass
                    else:
                        os.mkdir(os.path.join(logsdir, p))
            else:
                os.mkdir(logsdir)
                for p in levels:
                    os.mkdir(os.path.join(logsdir, p))

            logging_level_path = {}
            for i in levels:
                filename = os.path.join(logsdir, i, stamp)
                logging_level_path[i] = filename
            logger = logging.getLogger(schedule_name)
            for k, v in logging_level_path.items():
                handler = logging.handlers.TimedRotatingFileHandler(filename=v, when='MIDNIGHT', interval=1,
                                                                    backupCount=4, encoding="utf-8")
                handler.suffix = "%Y-%m-%d.log"
                handler.extMatch = r"^\d{4}-\d{2}-\d{2}.log$"
                handler.extMatch = re.compile(handler.extMatch)
                h_fmt = logging.Formatter(log_fmt)
                handler.setFormatter(h_fmt)
                if k in ["DEBUG", "INFO", "WARNING", "ERROR"]:
                    handler.setLevel(k)
                logger.addHandler(handler)
            logger.propagate = console_print
            return logger
        else:
            raise TypeError('logging_level support in:["DEBUG","INFO","WARNING","ERROR"]')
    else:
        raise NameError("logging_level expect list but get %s" % type(logging_level).__name__)
