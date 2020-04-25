import logging
import multiprocessing as mp
import os

"""
样例
"""

import file_processing


with open("../sample/sample.csv", 'w', encoding='utf8')as fw:
    for i in range(1000000):
        fw.write(str(i)+"\n")

abs_path = os.path.abspath(fw.name)
abs_path_dir = os.path.dirname(abs_path)


def line_process(line):
    line = line.rstrip()
    return "{}\n".format(line * 10)


if __name__ == '__main__':
    mp.freeze_support()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    file_processor = file_processing.FileProcessing(input_file=abs_path,
                                                    output_file=abs_path+".out",
                                                    line_call=line_process,
                                                    tmp_dir=os.path.join(abs_path_dir, "tmp"),
                                                    split_line_size=100000,
                                                    )
    print(file_processor.run())
    # (True, 12.15)
    print(file_processor.err_msg)
    # None
