import logging
import os
import subprocess
import time
from functools import wraps


def get_process_msg(std_msg):
    return std_msg.decode('utf8', 'ignore') if std_msg else "null"


def get_process_by_cmd(cmd, wait=True, logger=logging):
    """wait=True: Wait child process to terminate."""
    logger.info("cmd: {} execute".format(cmd))
    process = subprocess.Popen(cmd, shell=True,
                               stderr=subprocess.PIPE,
                               stdout=subprocess.PIPE)
    if not wait:
        return process
    stdout, stderr = process.communicate()
    stdout = get_process_msg(stdout)
    stderr = get_process_msg(stderr)
    logger.info("cmd:{} stdout: {}".format(cmd, stdout))
    if process.returncode:
        raise OSError("cmd: {} execute failure, stderr:{}"
                      .format(cmd, stderr))
    return process


def get_files_by_path(path):
    """遍历文件夹下全部的文件"""
    files = []
    for base, dirs, names in os.walk(path):
        files.extend([os.path.join(base, n) for n in names])
    return files
    # return [os.path.join(path, f) for f in os.listdir(path)]


def exc_redirect(code):
    """将异常转为标准输出"""
    def _handler(func):
        @wraps(func)
        def __handler(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger = kwargs.get("logger", logging)
                logger.error("{}: exc be format".format(func, code))
                logger.exception(e)
                return code
        return __handler
    return _handler


def file_encoding_validate(filename, encoding='utf8'):
    """文件编码验证"""
    with open(filename, 'rb')as fr:
        for line in fr:
            try:
                line.decode(encoding)
            except UnicodeDecodeError:
                return False
                # raise TypeError("file: {} encoding!={}"
                #                 .format(filename, encoding))
    return True


def time_count(func):
    @wraps(func)
    def count(*args, **kwargs):
        t = time.time()
        try:
            res = func(*args, **kwargs)
        finally:
            cnt = round(time.time() - t, 2)
        return res, cnt
    return count
