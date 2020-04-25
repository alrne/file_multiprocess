import shutil
import time
import multiprocessing as mp
import os
import logging

import process_pool
from utils import cmd_utils


class FileProcessing(object):
    def __init__(self, input_file, output_file, line_call,
                 split_line_size=10 * 1000,
                 worker_size=None,
                 tmp_dir="/tmp/",
                 overwrite=True,
                 logger=logging.getLogger()):
        """
        文件多线程处理
        :param input_file: 输入文件
        :param output_file: 输出文件
        :param line_call: 行处理函数
        :param split_line_size: 最大行数 default=100000
        :param worker_size: 并行最大进程数
        :param tmp_dir 文件缓存路径 default=/tmp/
        :param overwrite 覆盖输出文件
        :param logger: 日志
        """
        self.log = logger
        output_dir, filename = os.path.split(output_file)
        if not os.path.isdir(output_dir):
            raise ValueError("输出目录：{} 不存在"
                             .format(output_dir))
        if not filename:
            raise ValueError("输出文件 {} 没有文件名".format(output_file))
        if os.path.isfile(output_file) and not overwrite:
            raise ValueError("输出文件 {} 已存在,覆盖为否"
                             .format(output_file))
        if not os.path.isfile(input_file):
            raise ValueError("输入文件 {} 不存在"
                             .format(input_file))
        if not cmd_utils.file_encoding_validate(input_file):
            raise ValueError("输入文件 {} 不是utf8编码"
                             .format(input_file))
        if not getattr(line_call, "__call__"):
            raise ValueError("参数异常，接受func 但是传递了 {}"
                             .format(type(line_call)))
        if not isinstance(split_line_size, int):
            raise ValueError("切割行数大小必须是整数")
        if not os.path.isabs(tmp_dir):
            raise ValueError("缓存目录必须是绝对路径 以 / 开始")
        self._tmp_dir = os.path.join(tmp_dir, filename)
        if os.path.isdir(self._tmp_dir):
            raise ValueError("目录 {} 已存在" .format(self._tmp_dir))
        self.input_file = input_file
        self.output_dir = output_dir
        self.filename = filename
        self.overwrite = overwrite
        self._result_suffix = ".result"
        self._line_call = line_call
        self._line_size = split_line_size
        self._worker_size = worker_size or mp.cpu_count()
        self.return_code = None
        self._is_shutdown = False
        self.err_msg = None

    @staticmethod
    def file_part_handler(func, suffix, filename):
        """处理文件的主方法"""
        if not os.path.isfile(filename):
            raise OSError("文件块 {} 不存在".format(filename))
        fr = open(filename, 'r')
        fw = open(filename + suffix, 'w')
        try:
            for line in fr:
                fw.write(func(line))
        finally:
            fw.close()
            fr.close()
        return True

    def _shutdown(self):
        """回收 1.硬盘空间 2.内存空间"""
        if self._is_shutdown is True:
            return
        self._is_shutdown = True
        self.log.info("回收目录 {}".format(self._tmp_dir))
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def _split_input_file(self):
        os.makedirs(self._tmp_dir+"/")
        i = 0
        self._file_list = []
        with open(self.input_file, "r", encoding='utf8')as fr:
            cur_lines = 0
            fw = open(os.path.join(self._tmp_dir, str(i)), "w", encoding='utf8')
            self._file_list.append(fw.name)
            for line in fr:
                fw.write(line)
                cur_lines += 1
                if cur_lines == self._line_size:
                    cur_lines = 0
                    fw.close()
                    i += 1
                    fw = open(os.path.join(self._tmp_dir, str(i)), "w", encoding='utf8')
                    self._file_list.append(fw.name)
            fw.close()
        self.log.info("文件 {} 切割成功, 分割块为 {} 个".format(self.input_file, len(self._file_list)))
        return True

    def _handle_err(self, e):
        self.log.error("发生异常，任务终止")
        self.log.exception(e)
        self.return_code = False
        self._shutdown()
        raise e

    def _finished_tasks(self):
        if len(self._file_list) < self._worker_size:
            self._worker_size = len(self._file_list)
        self.log.info("进程数：{}" .format(self._worker_size))
        process_args = [(self._line_call, self._result_suffix, i)
                        for i in self._file_list]
        with process_pool.ProcessPool(size=self._worker_size,
                                      target=self.file_part_handler,
                                      iterable=process_args,
                                      logger=self.log,
                                      p_bar=True) as pool:
            self.return_code = pool.run()
            self.err_msg = pool.err_msg
        self._merge_output_file()

    @cmd_utils.time_count
    def run(self):
        """任务执行入口"""
        st = time.time()
        self._split_input_file()
        self._finished_tasks()
        self.log.info("数据处理共花费 {} 秒".format(round(time.time() - st, 2)))
        self._shutdown()
        return self.return_code

    def _merge_output_file(self):
        """合并输出文件"""
        if not self.return_code:
            self.log.info("文件处理失败，无需合并")
            return self.return_code
        output_file = os.path.join(self.output_dir, self.filename)
        if os.path.isfile(output_file) and self.overwrite:
            self.log.info("输出文件：{} 将被覆盖" .format(output_file))
        # 停顿1s 避免IO过慢时文件检查不通过
        time.sleep(1)
        with open(output_file, "w")as fw:
            for part in self._file_list:
                filename = part+self._result_suffix
                if not os.path.isfile(filename):
                    self._handle_err(OSError("任务合并时，发现文件块 {} 不存在".format(filename)))
                with open(filename, "r")as fr:
                    shutil.copyfileobj(fsrc=fr, fdst=fw)

    def __del__(self):
        """对象被销毁时执行"""
        return_code = getattr(self, "return_code", None)
        if return_code is None:
            self.return_code = False
        self._shutdown()

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.input_file)
