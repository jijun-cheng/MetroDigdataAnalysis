# coding=UTF-8
from pyhive import hive


def insertInto_table(host="localhost", database="", TableName="", sql=""):
    """

    :param host:
    :param database:
    :param TableName:
    :param sql: SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d GROUP BY d.Province ORDER BY `商品种类` DESC
    :return:
    """

    conn = hive.connect(host=host, database=database)
    insertSql = '''
          INSERT INTO {:s} ({:s})     
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