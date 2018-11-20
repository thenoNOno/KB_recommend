#!/usr/bin/env python
# -*- coding: utf-8 -*
import pandas as pd
import numpy as np
import hashlib
import pymysql
import json
import configparser
import time
import os
import re
from neo4j.v1 import GraphDatabase
from datetime import datetime


class carrier():
    """
    数据操作相关的工具类

    """
    def get_jdbc(self):
        config = configparser.ConfigParser()
        config.read("./mysql_link.conf")
        host = config.get("conf", "host")
        port = int(config.get("conf", "port"))
        user = config.get("conf", "user")
        passwd = config.get("conf", "passwd")
        db = config.get("conf", "db")
        charset = config.get("conf", "charset")
        jdbc = f'''jdbc:mysql://{host}:{port}/{db}?user={user}&password={passwd}&characterEncoding={charset}'''
        return jdbc

    def get_connect(self):
        config = configparser.ConfigParser()
        config.read("./mysql_link.conf")
        host = config.get("conf", "host")
        port = int(config.get("conf", "port"))
        user = config.get("conf", "user")
        passwd = config.get("conf", "passwd")
        db = config.get("conf", "db")
        charset = config.get("conf", "charset")
        conn = pymysql.connect(
            host=host
            ,port=port
            ,user=user
            ,passwd=passwd
            ,db=db
            ,charset=charset
        )
        return conn

    def run_query(self, query):
        """
        执行sql语句

        返回可能的查询结果
        """
        conn = self.get_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            if cursor.description is None:
                return True
            else:
                desc = cursor.description
            column = []
            for field in desc:
                column.append(field[0])
            results = []
            results.append(column)
            results.extend(cursor.fetchall())
            return results
        except Exception as error:
            print("错误信息:", error)
        finally:
            cursor.close()
            conn.commit()
            conn.close()

    def execute_data(self, sql, data):
        """
        批量执行sql,处理数据
        """
        conn = self.get_connect()
        cursor = conn.cursor()
        try:
            results = cursor.executemany(sql, data)
            return results
        except Exception as error:
            print("错误信息:", error)
        finally:
            cursor.close()          # 关闭cursor
            conn.commit()           # 提交
            conn.close()            # 关闭conn

    def get_driver(self):
        """
        获取neo4j的session

        """
        config = configparser.ConfigParser()
        config.read("./neo4j_link.conf")
        uri = config.get("conf", "uri")
        username = config.get("conf", "username")
        password = config.get("conf", "password")
        driver = GraphDatabase.driver(uri, auth=(username, password))
        return driver

    def run_cypher(self, cypher):
        """
        执行cypher

        """
        driver = self.get_driver()
        with driver.session() as session:
            with session.begin_transaction() as tran:
                run = tran.run(cypher)
                return run

    def save_csv(self, dataframe, filename, mode='w', header=True, sep=','):
        """
        将dataframe存储为csv

        """
        dataframe.to_csv(filename
                         ,mode=mode
                         ,header=header
                         ,index=False
                         ,sep=sep
                         ,encoding='utf8')
        return filename

    def save_txt(self, lists, filename, mode='a+'):
        """
        打开文件,写入数据

        """
        with open(filename, mode, encoding='utf8') as f:
            for line in lists:
                f.write(line)

    def scan_files(self, filepath, suffix):
        """
        根据路径与脚本后缀,扫描脚本

        """
        filelist = []
        print("开始扫描:'{0}'".format(filepath))
        try:
            for filename in os.listdir(filepath):
                if os.path.isdir(filepath + "/" + filename):
                    filelist.extend(self.scan_files(filepath + "/" + filename, suffix))
                else:
                    if filename.endswith(suffix):
                        filelist.append((filepath + "/" + filename))
            return filelist
        except Exception as error:
            print("错误信息:", error)


class writer(object):
    """
    数据写出相关的类

    """
    def save_data_csv(self, cypher, filename, mode='a', header=True):
        """
        执行cypher查询并存储为csv

        """
        cypher = cypher
        c = carrier()
        data = c.run_cypher(cypher).data()
        if data:
            print('数据正在处理')
        else:
            print('没有获取到数据', cypher)
        dataframe = pd.DataFrame(data)
        print(filename)
        c.save_csv(dataframe, filename, mode, header)
        return filename

    def save_d2v_txt(self, cypher, filename):
        """
        执行cypher查询,并将查询结果整理为供doc2vec使用的txt文档

        """
        cypher = cypher
        c = carrier()
        data = c.run_cypher(cypher).data()
        if data:
            print('数据正在处理')
        else:
            print('没有获取到数据')
        d2v = []
        for line in data:
            d2v.append(str(line['batch']))
            d2v.append(',')
            d2v.append(str(' '.join(str(s) for s in line['thing'])))
            d2v.append('\n')
        print(filename)
        c.save_txt(d2v, filename)
        return filename

    def save_w2v_txt(self, cypher, filename):
        """
        执行cypher查询,并将查询结果整理为供word2vec使用的txt文档

        """
        cypher = cypher
        c = carrier()
        data = c.run_cypher(cypher).data()
        if data:
            pass
        else:
            print('一次查询没有获取到数据')
        w2v = []
        for line in data:
            w2v.append(str(' '.join(str(s) for s in line['thing'])))
            w2v.append('\n')
        c.save_txt(w2v, filename)
        return filename

    def save_vector_csv(self, w2v_filename, filename, mode='w'):
        """
        将w2v向量整理为需要的csv格式

        """
        data = []
        with open(w2v_filename, 'r') as f:
            #next跳过第一行,因为它并不是词向量
            next(f)
            for line in f:
                tmp_line = line.replace('\n', '').split(' ', 1)
                tmp_id = tmp_line[0]
                vector = tmp_line[1].split(' ')
                norm = np.linalg.norm(vector)
                tmp_list = []
                tmp_list.append(tmp_id)
                tmp_list.append(norm)
                data.append(tmp_list)
        dataframe = pd.DataFrame(data)
        dataframe.columns = ['id', 'norm']
        c = carrier()
        c.save_csv(dataframe, filename, mode)
        return filename

    def load_vector_neo4j(self, filename, source='local'):
        """
        将w2v的权重从csv文件更新到neo4j

        """
        if source == 'local':
            batch = '{batchSize:10000, iterateList:true, parallel:true}'
            cypher = f'''
            CALL apoc.periodic.iterate(
            'CALL apoc.load.csv("{filename}") yield map as row return row'
            ,'match ()-[r]-() where id(r)=toInteger(row.id) SET r += row'
            ,{batch}
            )
            ;
            '''
            c = carrier()
            res = c.run_cypher(cypher)
        elif source == 'mysql':
            c = carrier()
            source = c.get_jdbc()
            timestamp = str(int(time.time()))
            table = timestamp+'_tmp_rid_norm'
            drop_sql = f'drop table if exists {table};'
            create_sql = f'create table {table}(id varchar(256), norm double);'
            load_sql = f'insert into {table} values(%s,%s)'
            data = pd.read_csv(filename, encoding='utf8', dtype='str').values.tolist()
            sql_0 = c.run_query(drop_sql)
            sql_1 = c.run_query(create_sql)
            if sql_0 and sql_1:
                c.execute_data(load_sql, data)
                select_sql = f'select * from {table};'
                batch = '{batchSize:10000, iterateList:true, parallel:true}'
                cypher = f'''
                CALL apoc.periodic.iterate(
                'CALL apoc.load.jdbc("{source}","{select_sql}") YIELD row return row'
                ,'match ()-[r]-() where id(r)=toInteger(row.id) SET r += row,r.etltime="{timestamp}" '
                ,{batch}
                )
                ;
                '''
                c.run_cypher(cypher)
            res = (table, cypher)
        else:
            res = False
        return res

    def load_term_neo4j(self, filename, label, label_end, source='local'):
        """
        """
        filename = filename
        label = label
        label_end = label_end
        check = '{'+f'''pid:row.{label}'''+'}'
        check_e = '{'+f'''pid:row.{label_end}'''+'}'
        check_n = '{'+f'''pid:row.{label_end}'''+'}'
        if source == 'local':
            batch = '{batchSize:10000, iterateList:false, parallel:true}'
            cypher = f'''
            CALL apoc.periodic.iterate(
            'CALL apoc.load.csv("{filename}") yield map as row return row'
            ,'merge (n:outside:{label} {check}) with *
            merge (n_e:outside:{label_end} {check_e}) with *
            merge (n_n:outside:entity {check_n}) with *
            merge (n)-[r:watching]->(n_e)-[:watching]->(n_n)
            set r.freq = coalesce(r.freq,0)+1 '
            ,{batch}
            )
            ;
            '''
            #print(cypher)
            c = carrier()
            res = c.run_cypher(cypher)
        elif source == 'mysql':
            c = carrier()
            source = c.get_jdbc()
            timestamp = str(int(time.time()))
            table = timestamp+'_tmp_rid_norm'
            drop_sql = f'drop table if exists {table};'
            create_sql = f'create table {table}({label} varchar(2000), {label_end} varchar(2000));'
            load_sql = f'insert into {table} values(%s,%s)'
            data = pd.read_csv(filename, encoding='utf8', dtype='str').values.tolist()
            sql_0 = c.run_query(drop_sql)
            sql_1 = c.run_query(create_sql)
            if sql_0 and sql_1:
                c.execute_data(load_sql, data)
                select_sql = f'select * from {table};'
                batch = '{batchSize:10000, parallel:false, iterateList:true}'
                cypher = f'''
                CALL apoc.periodic.iterate(
                'CALL apoc.load.jdbc("{source}","{select_sql}") YIELD row return row'
                ,'merge (n:outside:{label} {check}) with *
                merge (n_e:outside:{label_end} {check_e}) with *
                merge (n_n:outside:entity {check_n}) with *
                merge (n)-[r:watching]->(n_e)-[:watching]->(n_n)
                set r.freq = coalesce(r.freq,0)+1 '
                ,{batch}
                )
                ;
                '''
                c.run_cypher(cypher)
            res = (table, cypher)
        else:
            res = False
        return res

    def make_subfile(self, lines, head, filename, sub):
        """
        保存子文件

        """
        with open(filename, 'w', encoding='utf8') as f:
            if head != '':
                f.writelines([head])
            else:
                pass
            f.writelines(lines)
            return sub + 1

    def split_count(self, filename, count, header=True):
        """
        根据行数切分文件

        """
        [name, extension] = os.path.splitext(filename)
        filenames = []
        with open(filename, 'r', encoding='utf8') as f:
            if header is True:
                head = f.readline()
            elif header is False:
                head = ''
            else:
                head = header
            buff = []
            sub = 0
            for line in f:
                buff.append(line)
                if len(buff) == count:
                    subfilename = name+'_tmp_'+str(sub)+extension
                    sub = self.make_subfile(buff, head, subfilename, sub)
                    buff = []
                    filenames.append(subfilename)
            if len(buff) != 0:
                subfilename = name+'_tmp_'+str(sub)+extension
                sub = self.make_subfile(buff, head, subfilename, sub)
                filenames.append(subfilename)
        return filenames

    def save_txt_batch(self, data, filename, batch):
        """
        判断数组大小写出数据

        """
        c = carrier()
        if len(data) > batch:
            c.save_txt(data, filename, mode='a+')
            data = []
        else:
            pass
        return data

    def save_csv_batch(self, data, columns, filename, batch):
        """
        判断数组大小写出数据

        """
        c = carrier()
        if len(data) > batch:
            dataframe = pd.DataFrame(data)
            dataframe.columns = columns
            if os.path.exists(filename):
                header = False
            else:
                header = True
            c.save_csv(dataframe, filename, mode='a+', header=header)
            dataframe.drop(columns=columns, inplace=True)
            data = []
        else:
            pass
        return data

    def merge_csv(self, file_list, target_file, columns, mode='w'):
        if mode == 'a' and os.path.exists(target_file):
            header = False
        else:
            header = True
        data = pd.DataFrame(columns=columns)
        for line in file_list:
            file_data = pd.read_table(line, sep=",", encoding='utf8', dtype='str')
            data = data.append(file_data, ignore_index=True)
        c = carrier()
        c.save_csv(data, target_file ,mode , header=header)
        return target_file


class rule(object):
    """
    封装一些代码规则

    """
    def soft_remove(self, filename):
        #清理文件,重命名到remove_backup路径
        if os.path.exists(filename):
            remove_time = str(time.time())
            remove_backup = filename+remove_time+'.remove'
            os.rename(filename, remove_backup)
            return True
        else:
            pass

    def path_exists(self, filename):
        if os.path.exists(filename):
            return True
        else:
            print('文件不存在:', filename)

    def line_qualifier(self, line, qualifier='"', separator=',', column_separator=' '):
        line = ''+qualifier+line.replace(qualifier, '').replace(separator, '","').replace(column_separator, '" "')+qualifier
        return line

    def make_plan(self, timestamp, filepath, flag, suffix):
        c = carrier()
        todo_list = []
        files = c.scan_files(filepath, suffix)
        if files:
            pass
        else:
            return todo_list
        for line in files:
            name = os.path.basename(line)
            name_list = name.split('_', 1)
            sub_name = name_list[0]
            flag_name = name_list[1]
            batch = re.sub('[^0-9]', '', sub_name)
            if batch != '' and flag == flag_name:
                batch = int(batch)
                if batch < timestamp-1:
                    todo_list.append(line)
                else:
                    pass
            else:
                pass
        return todo_list

    def get_md5(self, value):
        print(value)
        string = str(value)
        mdo = hashlib.md5()
        mdo.update(string.encode("utf-8"))
        res = mdo.hexdigest()
        return res


def main():
    """
    程序执行入口

    """

if __name__ == '''__main__''':
    main()
