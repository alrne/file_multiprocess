# coding=utf8
from concurrent import futures
from concurrent.futures.process import BrokenProcessPool
from typing import Iterable
import logging
import threading

from tqdm import tqdm

from utils import tqdm_utils


class FuturesPoll(threading.Thread):
    def __init__(self, size, target, iterable, logger=None, p_bar=False):
        """
        进程池
        :param size: 并发进程数
        :param target: 处理函数
        :param iterable: 任务
        :param logger: 日志打印
        :param p_bar: 进度条
        """
        super().__init__()
        if logger is None:
            logger = logging.getLogger()
        self.log = logger
        if not isinstance(iterable, Iterable):
            raise ValueError("{} not Iterable".format(iterable))
        if not getattr(target, "__call__"):
            raise ValueError("{} not callback".format(target))
        self.size = size
        self.target = target
        self.iterable = iterable
        self.return_code = None
        self.exc_msg = None
        if p_bar:
            self.tqdm_obj = tqdm(iterable=iterable,
                                 file=tqdm_utils.TqdmRedirectLog(logger))
        else:
            self.tqdm_obj = None
        self.handler_lock = threading.Lock()
        self._futures = []
        self._pool = None

    def _shutdown(self):
        for f in self._futures:
            if f.done():
                continue
            try:
                f.cancel()
            except BrokenProcessPool as e:
                pass
        self._pool.shutdown(wait=False)

    def _callback(self, f):
        if self.return_code is False:
            return
        if f.exception():
            self.log.error("task has exc")
            self.set_exc(f.exception())
            task_res = False
        else:
            task_res = f.result
        if not task_res:
            self.return_code = False
            self._shutdown()
        with self.handler_lock:
            if self.tqdm_obj:
                self.tqdm_obj.update()

    def run(self):
        with futures.ProcessPoolExecutor(self.size) as pool:
            self._pool = pool
            for args in self.iterable:
                if isinstance(args, tuple):
                    self._futures.append(self._pool.submit(self.target, *args))
                else:
                    self._futures.append(self._pool.submit(self.target, args))
            for f in self._futures:
                f.add_done_callback(self._callback)
        return self.return_code

    def set_exc(self, exc):
        """将异常转为文本"""
        exc_msg = ""
        if isinstance(exc, BaseException):
            exc_msg += exc.__class__.__name__ + ": "
        self.exc_msg = exc_msg + str(exc)

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.target)
