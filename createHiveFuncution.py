# coding=UTF-8
from pyhive import hive  # 用于创建hive tabel

def run_mysql_query(conn, sql):
    with conn.cursor()  as cursor:
        cursor.execute(sql)
        # print(type(cursor.fetchall()))
        # print(type(cursor.fetchall()))
        return cursor.fetchall()

def create_table(host="localhost", database="", newTableName="", attributeList='''   '''):
    """
    :param host:
    :param database:
    :param newTableName:
    :param attributeList:  exp: `province` varchar(255) , `goods_type` varchar(255)
    :return:
    """
    print(attributeList)
    conn = hive.connect(host=host, database=database)
    if (newTableName,) not in list(run_mysql_query(conn, "show tables")):
        sql = '''
               create table `{:s}`.`{:s}`  ({:s})     
        '''.format(database, newTableName, attributeList)
        cursor = conn.cursor()
        cursor.execute(sql)
    conn.close()

    return


if __name__=="__main__":
    create_table(host="localhost", database="Metro_Hangzhou", newTableName="test_inputstation", attributeList='''   `stationID` varchar(255), `inputNums` varchar(255) ''')