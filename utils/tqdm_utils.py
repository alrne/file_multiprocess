# coding=utf8
import logging


class TqdmRedirectLog(object):

    def __init__(self, logger):
        assert getattr(getattr(logger, "info"), "__call__"), \
            "{} has not info callback".format(logger)
        self.log = logger

    def write(self, msg):
        self.log.info(msg)

    def close(self):
        pass

    def open(self):
        pass

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.log)
