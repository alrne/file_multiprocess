import multiprocessing as mp
import traceback
from typing import Iterable
import logging

from tqdm import tqdm

from utils import tqdm_utils


class ProcessPool(object):
    def __init__(self, size, target, iterable, logger=logging.getLogger(), p_bar=False):
        """
        进程池
        发生异常即终止全部任务
        :param size: 并行进程数
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
        if p_bar:
            self.tqdm_obj = tqdm(iterable=iterable,
                                 file=tqdm_utils.TqdmRedirectLog(logger))

            self.tqdm_lock = mp.Lock()
        else:
            self.tqdm_obj = None
        self.pool = None
        self.err_msg = None
        self.trace_msg = None

    def _callback(self, res):
        if self.tqdm_obj is None:
            return
        with self.tqdm_lock:
            self.tqdm_obj.update()

    def _err_callback(self, exc):
        self.set_exc(exc)
        self.pool.terminate()
        self.return_code = False

    def run(self):
        with mp.Pool(self.size) as pool:
            self.pool = pool
            for args in self.iterable:
                self.pool.apply_async(func=self.target,
                                      args=args,
                                      callback=self._callback,
                                      error_callback=self._err_callback)
            self.pool.close()
            self.pool.join()
        if self.return_code is None:
            self.return_code = True
        return self.return_code

    def set_exc(self, exc):
        """将异常转为文本"""
        err_msg = ""
        if isinstance(exc, BaseException):
            err_msg += exc.__class__.__name__ + ": "
        self.err_msg = err_msg + str(exc)
        self.trace_msg = traceback.format_exc()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if getattr(self, "pool"):
            self.pool.terminate()

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.target)
