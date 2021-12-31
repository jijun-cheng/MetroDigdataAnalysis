# coding:utf-8
# --------------------------------
# Created by coco  on 16/2/23
# ---------------------------------
# Comment: 主要功能说明 :初始化业务数据库

import os
import pyhs2
import subprocess
from updateMySQLFunction import *
def run_cmd(cmd):
    """调取shell脚本，在Linux执行程序"""
    status, result = subprocess.getstatusoutput(cmd)
    print(result)

# conn=pyhs2.connect(host="192.168.8.94",port=10000,authMechanism="PLAIN",user="hdfs")
# mysql_info={"host":"192.168.8.94","port":3306,"user":"root","passwd":"gc895316"}
# print (mysql_info)
def run_hive_query(conn,sql):
    with conn.cursor()  as cursor:
        cursor.execute(sql)
        return cursor.fetchall()
# def mysql_to_hive(host,port,user,passwd,database,table):
#     #os.system("hadoop fs -rm    -r /user/task/%s"%table)
#     if [database] not in run_hive_query("show databases"):
#         with conn.cursor() as cursor:
#             cursor.execute("create database " +database)
#     with conn.cursor() as cursor:
#         cursor.execute("use  "+database)
#     if [table] not in run_hive_query("show tables"):
#         os.system("sqoop   import --connect   jdbc:mysql://%s:%s/%s --username  %s   --password  %s --table %s  --hive-database  %s  -m 10 --create-hive-table --hive-import   --hive-overwrite "%(
#             host,port,database,user,passwd,table,database))
#     else:
#         os.system("sqoop   import --connect   jdbc:mysql://%s:%s/%s --username  %s   --password  %s --table %s  --hive-database  %s  -m 10 --hive-import   --hive-overwrite "%(
#             host,port,database,user,passwd,table,database))


def hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="",MySQL_tableName="", Hive_database="",Hive_tableName=""):

    # 首先清空目标数据表

    truncate_mysql_table(host=host, user=user,passwd=passwd, portStr="3306",portInt=3306, db=MySQL_database, tableName=MySQL_tableName)
    os.system("""sqoop export --connect jdbc:mysql://Alex:3306/{:s} --username {:s} -password {:s} --table {:s} --export-dir  hdfs://Alex:9000/user/hive/warehouse/{:s}.db/{:s}  -m 1 --input-fields-terminated-by '\\001' """.format(
         MySQL_database,user, passwd, MySQL_tableName, Hive_database, Hive_tableName
    ))



if __name__=="__main__":
    print("1")
    hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop", MySQL_database="metro_hangzhou",
        MySQL_tableName="test_inputstation", Hive_database="metro_hangzhou", Hive_tableName="test_inputstation")