# file_multiprocess

多进程文件处理工具类



python3.6

遇到一个业务，对一个文件处理的速度进行性能调优
虽然是个菜鸟，但果断想到切割文件，线程池分块处理再合并的思路。
做出了多线程进行测试时才发现多线程（python）去做计算密集型的任务速度还不如单线程的快，由此诞生了这个多进程工具类。
python多线程和多进程，并行和并发的问题，这里不在累述。
结论是：*多线程处理IO密集，多进程处理计算密集**

Multi-process file processing tool class

Encounter a business that performs performance tuning on the speed of a file processing
Although a rookie, but the decisive thought of cutting files, thread pool block processing and then merge ideas.
This multi-process tool class was born when multithreaded testing was made only to find that multithreaded (python) was not doing compute-intensive tasks as fast as single-threaded.
Python multithreaded and multi-process, parallel and concurrency problems are not re-described here.
The conclusion is: s/he multithreaded IO-intensive, multi-process computing-intensive.

项目目录
project mean
```shell
file_multiprocess
-- file_multiprocess.py
-- multiprocess_pool.py
-- utils\
  -- cmd_utils.py
  -- tqdm_utils.py
```

介绍（introduce） 
file_multiprocess.py
可独立使用的多进程文件处理，基于Linux实现
can work  to process file,support linux & windows

multiprocess_pool.py
进程池，封装模块multiprocess.Pool , 分配任务出现失败时,进程池关闭并返回
process pool, packaging multiprocess.Pool, if job error or has not result ,showdown the pool

项目待完善
20200418
