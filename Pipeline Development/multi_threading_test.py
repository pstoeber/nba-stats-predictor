import pymysql
import itertools
#import datetime
#import hashlib
#import codecs
import sys
import os
#import logging
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

def find_files(dir):
    file_list = []
    for root, dir, files in os.walk(dir):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list

def gen_db_conn():
    return pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_backup', autocommit=True)

def extract_file(file):
    with open(file, 'r') as infile:
        return [i for i in infile.readlines()]

def gen_cmd_str(cmd):
    return ''.join([i.replace('"', '\"') for i in cmd])

def sql_execute(file):
    conn = gen_db_conn()
    exe = conn.cursor()
    exe.execute('set session sql_mode = "NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER";')
    exe.execute(gen_cmd_str(extract_file(file)))
    #exe.execute(file)
    print(gen_cmd_str(extract_file(file)))
    return

def gen_threads(files):
    pool = Pool(2)
    results = pool.map(sql_execute, files)
    pool.close()
    pool.join()
    return





if __name__ == '__main__':
    conn = gen_db_conn()
    files = find_files(sys.argv[1])
    gen_threads(files)
    #sql_execute('set session sql_mode = "NO_ENGINE_SUBSTITUTION,NO_AUTO_CREATE_USER";')
    #files = ["""

    # insert into nba_stats_backup.advanced_box_stats (select `a`.`game_hash`,
    #                                                `p`.`player_id`,
    #                                                `a`.`MP`,
    #                                                `a`.`TS_PCT`,
    #                                                `a`.`EFG_PCT`,
    #                                                `a`.`3PAR`,
    #                                                `a`.`FTR`,
    #                                                `a`.`ORB_PCT`,
    #                                                `a`.`DRB_PCT`,
    #                                                `a`.`TRB_PCT`,
    #                                                `a`.`AST_PCT`,
    #                                                `a`.`STL_PCT`,
    #                                                `a`.`BLK_PCT`,
    #                                                `a`.`TOV_PCT`,
    #                                                `a`.`USG_PCT`,
    #                                                `a`.`ORTG`,
    #                                                `a`.`DRTG`
    #                                         from ((`nba_stats`.`advanced_box_stats` `a` join `nba_stats`.`player_info_view` `p` on ((`a`.`name` = `p`.`name`)))
    #                                                join `nba_stats`.`box_score_map_view` `bm`
    #                                                     on ((`a`.`game_hash` = `bm`.`game_hash`))));
    #
    #                                                     """,
    #          """
    #
    #          insert into nba_stats_backup.basic_box_stats (select `b`.`game_hash`,
    #                                                      `p`.`player_id`,
    #                                                      `b`.`MP`,
    #                                                      `b`.`FG`,
    #                                                      `b`.`FGA`,
    #                                                      `b`.`FG_PCT`,
    #                                                      `b`.`3P`,
    #                                                      `b`.`3PA`,
    #                                                      `b`.`3P_PCT`,
    #                                                      `b`.`FT`,
    #                                                      `b`.`FT_PCT`,
    #                                                      `b`.`ORB`,
    #                                                      `b`.`DRB`,
    #                                                      `b`.`TRB`,
    #                                                      `b`.`AST`,
    #                                                      `b`.`STL`,
    #                                                      `b`.`BLK`,
    #                                                      `b`.`TOV`,
    #                                                      `b`.`PF`,
    #                                                      `b`.`PTS`,
    #                                                      `b`.`PLUS_MINUS`
    #                                               from ((`nba_stats`.`basic_box_stats` `b` join `nba_stats`.`player_info_view` `p` on ((`b`.`name` = `p`.`name`)))
    #                                                      join `nba_stats`.`box_score_map_view` `bm`
    #                                                           on ((`b`.`game_hash` = `bm`.`game_hash`))));
    #
    #                                                           """]
