# !/usr/bin/python
# -*- coding: UTF-8 -*-


import json
import os
from pydbclib import connect as con
from loggerPro import logger

'''
@author:    anke
@contact:   anke.wang@foxmail.com
@file:      calculateConfidence.py
@time:      2020/5/9 2:41 下午

@desc:      
'''

from enum import Enum, unique


@unique
class Path(Enum):
    MAPPINGPATH = './mapping'
    SQLPATH = './sql'
    FIELDMAPPING = {
        "身份证": "sfzh",
        "身份证号": "sfzh",
        "用户名": "user_name",
        "微信": "user_name",
        "QQ": "user_name",
        "邮箱": "email",
        "手机号": "phoneno",
        "手机号码": "phoneno",
        "密码": "password",
        "密码-加密": "password",
        "明文密码": "password",
        "置信度": "confidence"
    }
    SOURCEMAPPING = {"12306": "12306",
                     "126": "126",
                     "163": "163",
                     "7k7k": "7k7k",
                     "acfun": "acfun",
                     "csdn": "csdn",
                     "renren": "renren",
                     "tianya": "tianya",
                     "xiaomi": "xiaomi",
                     "珍爱网": "zhenaiwang",
                     "52房地产": "52fangdichan",
                     "92hacker": "92hacker",
                     "118faka": "118faka",
                     "open": "open",
                     "zp": "zp",
                     "曹长青": "caochangqing",
                     "汉庭": "hanting",
                     "鲸鱼": "jingyu",
                     "缅华": "mianhua",
                     "台湾海外网": "taiwanhaiwaiwang",
                     "一亿": "yiyi",
                     "web": "web"}


class DrawMapping(object):
    def __init__(self, conn, outPutPath=Path.MAPPINGPATH.value):
        self.db = con(f"mysql+pymysql://{conn}")
        self.filePath = outPutPath
        self.fieldTransMapping = Path.FIELDMAPPING.value
        self.sourceTransMapping = Path.SOURCEMAPPING.value

    def _getSql(self, table):
        sql = f"""select 
                        t3.DB_ID,t3.DB_LOCATION_URI,t3.`NAME`DB_NAME,
                        t2.TBL_NAME,FROM_UNIXTIME(t2.CREATE_TIME,'%Y-%m-%d %H:%i:%S')CREATE_TIME,t1.file_source SOURCE,
                        t5.INTEGER_IDX COL_INDEX,t5.COLUMN_NAME COL_NAME,
                        case when t5.COLUMN_NAME='uuid' then 'uuid' else `COMMENT` end COL_COMMENT,
                        t5.TYPE_NAME COL_TYPE
                    from dangan.asset_managament_info t1 
                    left join hive.TBLS t2 on t2.TBL_NAME=t1.table_final_name
                    left join hive.DBS t3 on t3.DB_ID=t2.DB_ID
                    left join hive.SDS t4 on t2.SD_ID=t4.SD_ID
                    left join hive.COLUMNS_V2 t5 on t4.CD_ID=t5.CD_ID
                    where 1=1
                    AND t3.`NAME`='sgk_source'
                    -- AND t2.TBL_NAME='ssd_original_xiaomi_result'
                    ORDER BY DB_NAME,TBL_NAME,COL_INDEX"""
        sql = f"select * from dangan.tb_ml_temp_t1 where 1=1 and TBL_NAME='{table}'"
        return sql

    def _drawMapping(self, table):
        sql = self._getSql(table)
        lines = self.db.read(sql)
        jBase = {}
        jField = {"uuid": "uuid",
                  "sfzh": "",
                  "user_name": "",
                  "email": "",
                  "phoneno": "",
                  "password": "",
                  "explode_time": "explode_time",
                  "confidence": "confidence",
                  "source_table": "",
                  "source": ""}
        fields = []
        jRule = {}
        for line in lines:
            if line['col_comment'] in self.fieldTransMapping:
                jField[self.fieldTransMapping[line['col_comment']]] = line['col_name']
                if self.fieldTransMapping[line['col_comment']] == 'confidence':
                    jRule[line['col_name']] = True
            fields.append(line['col_name'])
            if line['source'] in self.sourceTransMapping:
                line['source'] = self.sourceTransMapping[line['source']]

            jField['source'] = line['source']
            jField['source_table'] = line['tbl_name']

            jBase['source'] = line['source']
            jBase['table'] = line['tbl_name']
            jBase['database'] = line['db_name']
            jBase['fields'] = fields
            jBase['fieldMapping'] = jField
            jBase['rule'] = jRule
        logger.info(f"正在处理【{jBase['database']}.{jBase['table']}】表...")
        loads = json.dumps(jBase, ensure_ascii=False, indent=4)
        if not os.path.isdir(self.filePath):
            os.makedirs(self.filePath)
        file = os.path.join(self.filePath, line['tbl_name'] + '.json')
        with open(file, mode="w+", encoding="utf-8") as fd:
            fd.write(loads)
            fd.flush()
            sql = f"update dangan.asset_managament_info set is_load=1 where tbl_name='{line['tbl_name']}'"
            sql = f"update dangan.tb_ml_temp_t1 set is_load=1 where tbl_name='{line['tbl_name']}'"
            execute = self.db.execute(sql, autocommit=True)
            if execute > 0:
                logger.info(f"【{jBase['database']}.{jBase['table']}】表构建mapping成功！")

    def build_from_file(self, tableFile):
        """
        通过读取table.txt文件来构建表mapping
        :param tableFile:
        :return:
        """
        with open(tableFile) as tbls:
            if not bool(tbls):
                logger.warning(f'没有找到需要构建的表，请检查table.txt文件或数据库')
            for tbl in tbls.readlines():
                tbl = tbl.replace('\n', '')
                if (tbl.strip(' ') == '' or tbl.startswith('#')): continue
                self._drawMapping(tbl)

    def build_from_db(self):
        """
        通过查询数据库，来构建表mapping
        :return:
        """
        # tbls = self.db.read(f'select distinct table_final_name from dangan.asset_managament_info where is_load=0')
        tbls = self.db.read(f'select distinct tbl_name from dangan.tb_ml_temp_t1 where is_load=0')
        for tbl in tbls.get_all():
            self._drawMapping(tbl['tbl_name'])


class DrawSql(object):
    def __init__(self, fileName, conn, sqlPath=Path.SQLPATH.value, mappingPath=Path.MAPPINGPATH.value):
        self.sqlPath = sqlPath
        self.mappingPath = mappingPath
        self.db = con(f"mysql+pymysql://{conn}")
        self.fileName = fileName
        # 校验配置的mapping数据格式
        with open(self.fileName, 'r', encoding='utf-8') as f:
            # 将类文件对象中的JSON字符串直接转换成Python字典
            self.jBase = json.load(f)
            # 获取json中的表信息
            self.source = self.jBase['source']
            self.database = self.jBase['database']
            self.table = self.jBase['table']
            self.fields = self.jBase['fields']
            # 字段映射成身份证号、邮箱、手机号、、、
            self.jField = self.jBase['fieldMapping']
            self.uuid = self.jField['uuid']
            self.sfzh = self.jField['sfzh']
            self.user_name = self.jField['user_name']
            self.email = self.jField['email']
            self.phoneno = self.jField['phoneno']
            self.password = self.jField['password']
            self.explode_time = self.jField['explode_time']
            self.confidence = self.jField['confidence']
            self.source_table = self.table

    def _ruleMatch(self, col, is_confidence):
        """
        规则转换，对json定义的字段规则进行转换,输出转换后的字段
        :param col: 需要做规则转换的列名称
        :param rule:confidence：true代表置信度需要重新计算,,该规则根据实际情况可以不断新增
        :return: 根据规则转换之后的列名称
        """
        c = ''
        if (is_confidence is None):
            c = col
        elif (is_confidence):
            """
                首先判断身份证手机号,俩都有               0.9
                有身份证或手机号其中一个                  0.8
                没有身份证手机号，判断邮箱，有邮箱          0.5
                没有身份证手机号没有邮箱，有用户名和密码	   0.5
                没有身份证手机号没有邮箱没有密码，只有用户名  0.3
                其他	                                   0.2
            """
            c = 'case'
            if (self.phoneno != '' and self.sfzh != ''):
                c = f"{c} when length(trim({self.phoneno}))=11 and substr(trim({self.phoneno}),1,1)='1' and length(trim({self.sfzh})) in (15,18) then '0.9' " \
                    f"when length(trim({self.phoneno}))=11 and substr(trim({self.phoneno}),1,1)='1' or length(trim({self.sfzh})) in (15,18) then '0.8'"
            elif (self.phoneno == '' and self.sfzh != ''):
                c = f"{c} when length(trim({self.sfzh})) in (15,18) then '0.8'"
            elif (self.phoneno != '' and self.sfzh == ''):
                c = f"{c} when length(trim({self.phoneno}))=11 and substr(trim({self.phoneno}),1,1)='1' then '0.8'"
            if (self.email != ''):
                c = f"{c} when upper(trim({self.email})) like '%.COM%s' then '0.5'"
            if (self.user_name != '' and self.password != ''):
                c = f"{c} when trim({self.user_name})!='' and trim({self.password})!='' then '0.5'"
            if (self.user_name != ''):
                c = f"{c} when trim({self.user_name})!='' then '0.3' "
            if (self.sfzh == '' and self.phoneno == ''
                    and self.email == '' and self.user_name == ''):
                c = "'0.2'"
            else:
                c = f"{c} else '0.2' end as {col}"
        else:
            c = col
        return c

    def _spellSql(self):
        """
        拼接要插入目标表的sql
        :param self:
        :return: sql
        """
        col = []
        # 获取json中的字段，并拼接字符串
        for v in self.fields:
            # 获取json中配置的字段规则，并进行字段转换
            rul = self.jBase['rule'].get(v)
            v_ = self._ruleMatch(v, rul)
            v_ = v_ if v_ != '' else 'null'
            col.append(v_)
        c2_ = ','.join(col)

        sql = f"insert overwrite table {self.database}.{self.table} " \
              f"({','.join(self.fields)}) " \
              f"select {c2_} from {self.database}.{self.table};"
        return sql

    def start(self):
        """
        开始构建sql
        :return:
        """
        file = os.path.join(self.sqlPath, self.table + '.sql')
        if not os.path.isdir(self.sqlPath):
            os.makedirs(self.sqlPath)
        with open(file, mode="w+", encoding="utf-8") as fd:
            write = fd.write(self._spellSql())
            if write:
                # sql = f"update dangan.table_final_name set is_load=2 where tbl_name='{self.table}'"
                sql = f"update dangan.tb_ml_temp_t1 set is_load=2 where tbl_name='{self.table}'"
                execute = self.db.execute(sql, autocommit=True)
                if execute:
                    logger.info(f"【{self.database}.{self.table}】表构建SQL成功！")
                    os.remove(os.path.join(self.mappingPath, self.table + '.json'))


if __name__ == '__main__':
    # connect = 'root:123456@172.20.1.11:3306'
    connect = 'root:123456@127.0.0.1:3306'
    DrawMapping(conn=connect).build_from_db()

    for dirPath, dirNames, fileNames in os.walk(Path.MAPPINGPATH.value):
        for fileName in fileNames:
            if fileName.endswith('json'):
                i = os.path.join(dirPath, fileName)
                with open(i, 'r') as f:
                    extSql = DrawSql(f.name, conn=connect).start()
