# file_multiprocess

多进程文件处理工具类



python3.6

切割文件，进程池处理，再合并
**多线程处理IO密集，多进程处理计算密集**


项目目录
```shell
file_multiprocess
-- file_processing.py
-- process_pool.py
-- utils\
  -- cmd_utils.py
  -- tqdm_utils.py
```

介绍：
file_processing.py
多进程文件处理

process_pool.py
封装multiprocess.Pool , 分配任务出现失败时,直接关闭
