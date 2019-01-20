import pymysql
import itertools
import codecs
import sys
import os
import re
#import logging
from functools import partial
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

def find_files(dir):
    file_list = []
    for root, dir, files in os.walk(dir):
        for file in files:
            if not re.search(r'.DS_Store', file):
                file_list.append(os.path.join(root, file))
    return file_list

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_backup', autocommit=True)

def extract_file(file):
    with open(file, 'r') as infile:
        return [i.strip('\n') for i in infile.readlines()]

def gen_cmd_str(cmd):
    return r''.join([i for i in cmd])

def sql_execute(file, flag):
    conn = gen_db_conn()
    exe = conn.cursor()
    if flag == 1:
        exe.execute('set session sql_mode = "NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER";')
        exe.execute(gen_cmd_str(extract_file(file)))
        exe.execute('set session sql_mode = default;')
    else:
        exe.execute(gen_cmd_str(extract_file(file)))
    return

def gen_threads(files, flag):
    pool = Pool()
    results = pool.map(partial(sql_execute, flag=flag), files)
    pool.close()
    pool.join()
    return

if __name__ == '__main__':
    primary_files = find_files(sys.argv[1])
    for file in primary_files:
        sql_execute(file, 0)

    multi_thread_files = find_files(sys.argv[2])
    gen_threads(multi_thread_files, 1)
