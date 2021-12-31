# coding=UTF-8
import pymysql


# 打开数据库连接

def run_mysql_query(conn, sql):
    with conn.cursor()  as cursor:
        cursor.execute(sql)
        # print(type(cursor.fetchall()))
        # print(type(cursor.fetchall()))
        return cursor.fetchall()


def create_mysql_table(host='localhost', user='root', passwd='root', port=3306, db='taobao_data_result',newtableName="", sql=""):
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
    db = pymysql.connect(host=host, user=user, passwd=passwd, port=port, db=db)

    print(list(run_mysql_query(db, "show tables")))
    if (newtableName,) not in list(run_mysql_query(db, "show tables")):
    # 使用 cursor() 方法创建一个游标对象 cursor
        cursor = db.cursor()

        # 使用 execute() 方法执行 SQL，如果表存在则删除
        cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

        cursor.execute(sql)

    # 关闭数据库连接
    db.close()

    return


if __name__=="__main__":
    create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
                       newtableName="test_inputstation", sql=''' create table `metro_hangzhou`.`test_inputstation`  (
  `stationID` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `inputNums` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic; ''')