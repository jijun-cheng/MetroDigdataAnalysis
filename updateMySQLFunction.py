# coding=UTF-8
import pymysql

def truncate_mysql_table(host='localhost', user='root', passwd='root', portStr="3306",portInt=3306, db='taobao_data_result', tableName="",):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = pymysql.connect(host=host,user=user,passwd=passwd,port=portInt,db=db)
    remove = "truncate table {:s}".format(tableName)

    showresult = ''' 
        select * from {:s}
    '''.format(tableName)
    cursor = conn.cursor()

    cursor.execute(remove)

    print(cursor.fetchall())
    conn.close()
    return