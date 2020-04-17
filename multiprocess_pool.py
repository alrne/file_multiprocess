# coding=utf8
import multiprocessing as mp
from typing import Iterable
import logging
import threading

from tqdm import tqdm

from utils import tqdm_utils


class MultiprocessPool(threading.Thread):
    def __init__(self, size, target, iterable, logger=logging.getLogger(), p_bar=False):
        """
        进程池
        :param size: 并发进程数
        :param target: 处理函数
        :param iterable: 任务
        :param logger: 日志打印
        :param p_bar: 进度条
        """
        super().__init__()
        if not isinstance(iterable, Iterable):
            raise ValueError("{} not Iterable".format(iterable))
        if not getattr(target, "__call__"):
            raise ValueError("{} not callback".format(target))
        self.size = size
        self.target = target
        self.iterable = iterable
        self.log = logger
        self.return_code = None
        self.exc_msg = None
        if p_bar:
            self.tqdm_obj = tqdm(iterable=iterable,
                                 file=tqdm_utils.TqdmRedirectLog(logger))
        else:
            self.tqdm_obj = None
        self.handler_lock = threading.Lock()

    def _callback(self, res):
        if not self.tqdm_obj:
            return
        with self.handler_lock:
            self.tqdm_obj.update()

    def _err_callback(self, pool_):
        def _callback(exc):
            self.set_exc(exc)
            pool_.terminate()
            self.return_code = False
        return _callback

    def run(self):
        with mp.Pool(self.size) as pool:
            for args in self.iterable:
                if not isinstance(args, tuple):
                    args = (args, )
                pool.apply_async(self.target, args=args,
                                 callback=self._callback,
                                 error_callback=self._err_callback(pool))
            pool.close()
            pool.join()
        if self.return_code is None:
            self.return_code = True
        return self.return_code

    def set_exc(self, exc):
        """将异常转为文本"""
        exc_msg = ""
        if isinstance(exc, BaseException):
            exc_msg += exc.__class__.__name__ + ": "
        self.exc_msg = exc_msg + str(exc)

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.target)

