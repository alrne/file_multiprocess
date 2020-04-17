# coding=utf8
import platform
import shutil
import time
import multiprocessing as mp
import os
import logging
import queue

from utils import cmd_utils


class FileMultiprocess(object):
    def __init__(self, input_file, output_file, line_call,
                 split_line_size=10 * 1000,
                 worker_size=None,
                 tmp_dir="/tmp/",
                 overwrite=True,
                 logger=logging):
        """
        文件多线程处理
        work on linux
        :param input_file: 输入文件
        :param output_file: 输出文件
        :param line_call: 行处理函数
        :param split_line_size: 最大行数 default=100000
        :param worker_size: 并行最大进程数
        :param tmp_dir 文件缓存路径 default=/tmp/
        :param overwrite 覆盖输出文件
        :param logger: 日志模块
        """
        if platform.system() != "Linux":
            raise TypeError("{} 仅支持Linux平台".format(self.__class__.__name__))
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
        os.makedirs(self._tmp_dir)
        self.input_file = input_file
        self.output_dir = output_dir
        self.filename = filename
        self.overwrite = overwrite
        self._line_call = line_call
        self._line_size = split_line_size
        self._worker_size = worker_size or mp.cpu_count()
        self._result_suffix = ".result"
        self._file_queue = mp.Queue()
        self._part_tuple = None
        self._event = mp.Event()
        self._processors = []
        self.return_code = None
        self._is_shutdown = False

    def _shutdown(self):
        """回收 1.硬盘空间 2.内存空间"""
        if self._is_shutdown is True:
            return
        self._is_shutdown = True
        self._event.set()
        self.log.info("回收目录 {}".format(self._tmp_dir))
        shutil.rmtree(self._tmp_dir, ignore_errors=True)
        pids = []
        for p in self._processors:
            try:
                p.terminate()
                pids.append(p.pid)
            except OSError:
                pass
        self.log.info("回收进程 {}".format(pids))

    def _split_input_file(self):
        cmd_utils.get_process_by_cmd("mkdir -p {}".format(self._tmp_dir), logger=self.log)
        cmd_utils.get_process_by_cmd("split -l {} -a 6 {} {}/part"
                                     .format(self._line_size, self.input_file, self._tmp_dir),
                                     logger=self.log)
        part_list = cmd_utils.get_files_by_path(self._tmp_dir)
        part_list.sort()
        [self._file_queue.put(f) for f in part_list]
        self._part_tuple = tuple(part_list)
        self.log.info("文件 {} 切割成功, 分割块为 {} 个".format(self.input_file, len(self._part_tuple)))

    def main(self):
        """处理文件的主方法"""
        cur_info = "进程 pg-{} p-{}" .format(os.getpgid(os.getpid()), os.getpid())
        self.log.info("{} 开始工作" .format(cur_info))
        while True:
            try:
                file_name = self._file_queue.get(timeout=10)
            except queue.Empty:
                self.log.info("{} 结束工作" .format(cur_info))
                break
            if not os.path.isfile(file_name):
                self._handle_err(OSError("文件块 {} 不存在".format(file_name)))
                continue
            fr = open(file_name, 'r')
            fw = open(file_name + self._result_suffix, 'w')
            try:
                for line in fr:
                    try:
                        fw.write(self._line_call(line))
                    except Exception as e:
                        self._handle_err(e)
            finally:
                fw.close()
                fr.close()

    def _handle_err(self, e):
        self.log.error("发生异常，任务终止")
        self.log.exception(e)
        self._shutdown()
        self.return_code = False
        raise e

    def _process_file(self):
        """
        创建子进程，处理文件块，按序合并产出输出文件
        :return: execute result: <type>boolean
        """
        if self._file_queue.qsize() < self._worker_size:
            self._worker_size = self._file_queue.qsize()
        self.log.info("进程数：{}" .format(self._worker_size))
        for i in range(self._worker_size):
            p = mp.Process(target=self.main)
            p.start()
            self._processors.append(p)
        for p in self._processors:
            p.join()
        if self._event.is_set():
            self.log.warning("任务执行中止，无需处理业务数据")
            self.return_code = False
            return False
        return self._merge_output_file()

    def run(self):
        """任务执行入口"""
        st = time.time()
        self._split_input_file()
        self._process_file()
        self.log.info("数据处理共花费 {} 秒".format(round(time.time() - st, 2)))
        self._shutdown()

    def _merge_output_file(self):
        """合并输出文件"""
        output_file = os.path.join(self.output_dir, self.filename)
        if os.path.isfile(output_file) and self.overwrite:
            self.log.info("输出文件：{} 将被覆盖" .format(output_file))
            os.remove(output_file)
        # 停顿1s 避免IO过慢时文件检查不通过
        time.sleep(1)
        for part in self._part_tuple:
            filename = part+self._result_suffix
            if not os.path.isfile(filename):
                self._handle_err(OSError("任务合并时，发现文件块 {} 不存在".format(filename)))
            cmd_utils.get_process_by_cmd("cat {} >> {}".format(filename, output_file))
        return True

    def __del__(self):
        """对象被销毁时执行"""
        if self.return_code is None:
            self.return_code = False
        self._shutdown()

    def __repr__(self):
        return "<{} {}>" .format(self.__class__.__name__, self.input_file)
