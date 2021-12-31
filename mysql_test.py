import pymysql
# 打开数据库连接


def createORupdate_mysql_table(host='localhost', user='root', passwd='root', port=3306, db='taobao_data_cjj1', newtableName="", parameterList=""):
    '''

    :param host:
    :param user:
    :param passwd:
    :param port:
    :param db:
    :param parameterList:   `goods_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                        `province` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
    :return:
    '''
    # 打开数据库连接
    db = pymysql.connect(host=host,user=user,passwd=passwd,port=port,db=db)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    # 使用 execute() 方法执行 SQL，如果表存在则删除
    cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

    # 使用预处理语句创建表
    sql = """create table `{:s}`.`{:s}`  ({:s}) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic""".format(db, newtableName,parameterList)

    cursor.execute(sql)

    # 关闭数据库连接
    db.close()

    return

