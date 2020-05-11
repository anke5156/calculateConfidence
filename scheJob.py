#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import subprocess
import time
from functools import wraps
from pydbclib import connect as con
from loggerPro import logger
from cmdThread import CmdThread
from calculateConfidence import DrawMapping, DrawSql, Path

'''
@author:    anke
@contact:   anke.wang@foxmail.com
@file:      scheJobHip.py
@time:      2020/4/23 3:11 PM

@desc:      
'''


def ScheJobHip(func):
    @wraps(func)
    def wrapper(*args, **kargs):
        sta, res = subprocess.getstatusoutput('ps -ef|grep scheJobHip.py')
        # print(res)
        numrows = 0
        for line in res.split('\n'):
            if line.find('Python scheJobHip.py') != -1:
                numrows = numrows + 1
        if numrows > 1:
            logger.info('当前程序正在运行，不予执行!')
        else:
            return func(**kargs)

    return wrapper


@ScheJobHip
def tick():
    mark = str(time.time())
    logger.info('%s' % mark)

    # 1.先建mapping
    logger.info('==============================开始构建mapping==============================')
    expMapping = DrawMapping(conn=connect)
    expMapping.build_from_db()

    # 2.根据mapping构建sql
    logger.info('==============================开始构建SQL==============================')
    for root, dirs, files in os.walk(Path.MAPPINGPATH.value):
        for file in files:
            if file.endswith('json'):
                file_ = os.path.join(root, file)
                with open(file_, 'r') as f:
                    expSql = DrawSql(f.name, conn=connect)
                    logger.info(f'正在处理【{f.name}】文件')
                    expSql.start()
        if os.path.isdir(Path.MAPPINGPATH.value):
            os.removedirs(Path.MAPPINGPATH.value)
    # 3.开始执行sql
    logger.info('==============================开始执行SQL文件==============================')
    threadList = []
    cnt = 1
    db = con(f"mysql+pymysql://{connect}")
    fileName = []
    for root, dirs, files in os.walk(Path.SQLPATH.value):
        for file in files:
            if file.endswith('sql'):
                fileName.append(file)
                file_ = os.path.join(root, file)
                with open(file_, 'r') as f:
                    logger.info('正在处理【%s】文件' % f.name)
                    thr = CmdThread(cnt, f'hive -f "{f.name}"')
                    threadList.append(thr)
    for t in threadList:
        t.start()
    for t in threadList:
        t.join()
        if t.isSuccess:
            table = str(t.cmd).split('/')[2].split('.')[0]
            sql = f"update dangan.tb_ml_temp_t1 set is_load=3 where tbl_name='{table}'"
            execute = db.execute(sql, autocommit=True)
            if execute > 0:
                os.remove(os.path.join(Path.SQLPATH.value, table + '.sql'))
    if os.path.isdir(Path.SQLPATH.value):
        os.removedirs(Path.SQLPATH.value)
    logger.info('==============================All Job Done!==============================')
    # splitlog('../logs/hipdataload.log', mark)


if __name__ == '__main__':
    # LoggerPro().config()
    # connect = 'root:123456@172.20.1.11:3306'
    connect = 'root:123456@127.0.0.1:3306'
    tick()
