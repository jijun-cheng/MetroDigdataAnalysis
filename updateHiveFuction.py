# coding=UTF-8
from pyhive import hive  # 用于创建hive tabel
import os
from sqoop_hive2mysql import *

def run_hive_query(conn, sql):
    with conn.cursor()  as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def initUpdate_data_table(host="localhost", database="", dataTableName="", localDataPath=""):
    conn = hive.connect(host=host, database=database)
    updateSql = '''
            load data local inpath '{:s}' into table {:s}.{:s}  
    '''.format(localDataPath, database, dataTableName)
    cursor = conn.cursor()
    cursor.execute(updateSql)
    conn.close()

    return


def overwriteUpdate_data_table(host="localhost", database="", dataTableName="", localDataPath=""):
    conn = hive.connect(host=host, database=database)
    updateSql = '''
            load data local inpath '{:s}' overwrite into table {:s}.{:s}
    '''.format(localDataPath, database, dataTableName)
    cursor = conn.cursor()
    cursor.execute(updateSql)
    conn.close()

    return


def initUpdate_result_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)
    remove = "truncate table {:s}".format(TableName)

    insertSql = '''
          INSERT INTO {:s} {:s}    
    '''.format(TableName, sql)


    showresult = ''' 
        select * from {:s}
    '''.format(TableName)
    cursor = conn.cursor()

    cursor.execute(remove)
    cursor.execute(insertSql)
    cursor.execute(showresult)
    print(cursor.fetchall())
    conn.close()
    return


def overwriteUpdate_result_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)
    remove = "truncate table {:s}".format(TableName)

    insertSql = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName, sql)

    showresult = ''' 
        select * from {:s}
    '''.format(TableName)
    cursor = conn.cursor()

    cursor.execute(remove)
    cursor.execute(insertSql)
    cursor.execute(showresult)
    print(cursor.fetchall())
    conn.close()
    return

def overwrite_Four_Update_result_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)
    remove = "truncate table {:s}".format(TableName)

    insertSqlA1 = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName,'''select count(DISTINCT stationID ) from record_0101''')

    insertSqlA2 = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName, '''select count(DISTINCT deviceID) from record_0101''')

    insertSqlA3 = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName, '''select count(DISTINCT userID) from record_0101''')

    insertSqlA4 = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName, '''select count(DISTINCT time) from record_0101''')

    cursor = conn.cursor()

    cursor.execute(remove)
    cursor.execute(insertSqlA1)
    cursor.execute(insertSqlA2)
    cursor.execute(insertSqlA3)
    cursor.execute(insertSqlA4)

    print(cursor.fetchall())
    conn.close()
    return

def singleInsert_result_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)

    insertSql = '''
          INSERT INTO {:s} {:s}   
    '''.format(TableName, sql)

    showresult = ''' 
        select * from {:s}
    '''.format(TableName)
    cursor = conn.cursor()


    cursor.execute(insertSql)
    cursor.execute(showresult)
    print(cursor.fetchall())
    conn.close()
    return

def truncate_result_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)
    remove = "truncate table {:s}".format(TableName)

    cursor = conn.cursor()

    cursor.execute(remove)
    # cursor.execute(insertSql)
    # cursor.execute(showresult)
    conn.close()
    return

if __name__=="__main__":
    initUpdate_result_table(host="localhost", database="metro_hangzhou", TableName="test_inputstation", sql="select stationID,count(*) as nums from record_0101  where status='1'  group by stationID  ORDER BY nums DESC")