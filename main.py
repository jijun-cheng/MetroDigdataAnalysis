# coding=UTF-8
from HiveSQLs import *
from createHiveFuncution import *
from updateHiveFuction import *
import pymysql
from createMysqlTable import *
from sqoop_hive2mysql import  *
import os
import time
# from display_pyecharts_refine import *
def run_mysql_query(conn, sql):
    with conn.cursor()  as cursor:
        cursor.execute(sql)
        # print(type(cursor.fetchall()))
        # print(type(cursor.fetchall()))
        return cursor.fetchall()

def initHiveTables():
    # createHiveFuncution.create_table(host="localhost", database="taobao_data",
    # newTableName="test_table3", attributeList='''`province` varchar(255) , `goods_type` varchar(255)''')

    # 0. 上传源数据
    initUpdate_data_table(host="localhost", database="metro_hangzhou", dataTableName="record_test",
                          localDataPath="/root/Metro_Hangzhou/Metro_train_m/m_record_2019-01-01.csv")

    # # 1. 按日期统计人流量-柱状图
    # #  1.1 Hive操作
    # # 1.1创建Hive表
    # #######  需要进行判断，如果存在则跳过
    # create_table(host="localhost", database="taobao_data1", newTableName="goods_c",
    #              attributeList=''' `province` varchar(255) , `goods_type` varchar(255) ''')
    # # 查询并插入
    # initUpdate_result_table(host="localhost", database="taobao_data1", TableName="goods_c",
    #                  sql='''SELECT count( DISTINCT d.item_category ) as `商品种类`,d.Province `所属地` from `data` as d
    #                  GROUP BY d.Province ORDER BY `商品种类` DESC''')
    # # 导出到Mysql
    # #  1.2 MySQL操作
    # create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_goods_c", sql=''' create table `taobao_data_result`.`sql_goods_c`  ( `goods_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
    #  `province` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
    # ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
    # ''')
    # #  1.3 Sqoop : Hive -> MySQL
    # hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_goods_c", Hive_database="taobao_data1",Hive_tableName="goods_c")

    # 2. 进站人次排行（站点压力情况）柱状图
    #  2.1 Hive操作
    # 2.1.1 创建Hive表
    #######  需要进行判断，如果存在则跳过
    create_table(host="localhost", database="Metro_Hangzhou", newTableName="inputStation_Nums",
                 attributeList='''   `stationID` varchar(255), `inputNums` varchar(255) ''')
    # 查询并插入
    initUpdate_result_table(host="localhost", database="metro_hangzhou", TableName="inputStation_Nums",
                            sql="select stationID,count(*) as nums from record_0101  where status='1'  group by stationID  ORDER BY nums DESC")
    # 导出到Mysql
    #  2.2 MySQL操作
    create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
                       newtableName="inputStation_Nums", sql=''' create table `metro_hangzhou`.`inputStation_Nums`  (
  `stationID` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `inputNums` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic''')

    #  2.3 Sqoop : Hive -> MySQL
    hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop", MySQL_database="metro_hangzhou",
        MySQL_tableName="inputStation_Nums", Hive_database="metro_hangzhou", Hive_tableName="inputstation_nums")

#     # 3. 统计各省份中访问量（visit_UV）
#     #  1.1 Hive操作
#     # 1.1创建Hive表
#     #######  需要进行判断，如果存在则跳过
#     create_table(host="localhost", database="taobao_data1", newTableName="visit_uv",
#                  attributeList=''' `buy_type` varchar(255) , `brow_num` varchar(255), `province` varchar(255) ''')
#     # 查询并插入
#     initUpdate_result_table(host="localhost", database="taobao_data1", TableName="visit_uv",
#                      sql='''SELECT 'UV', u. `用户数量`,u.`Province` FROM (SELECT COUNT(DISTINCT user_id) AS `用户数量`,`Province` FROM `data` GROUP BY `Province`) u''')
#     # 导出到Mysql
#     #  1.2 MySQL操作
#     create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_visit_uv", sql=''' create table `taobao_data_result`.`sql_visit_uv`  (
#   `buy_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `brow_num` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `province` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#
#
#     ''')
#     #  1.3 Sqoop : Hive -> MySQL
#     hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_visit_uv", Hive_database="taobao_data1",Hive_tableName="visit_uv")
#
#
#     # 4. 每日访问量统计（a_day）
#     #  1.1 Hive操作
#     # 1.1创建Hive表
#     #######  需要进行判断，如果存在则跳过
#     create_table(host="localhost", database="taobao_data1", newTableName="a_day",
#                  attributeList=''' `brow_time` varchar(255) , `brow_num` varchar(255)''')
#     # 查询并插入
#     initUpdate_result_table(host="localhost", database="taobao_data1", TableName="a_day",
#                      sql='''SELECT `time`,count(*) as `t` from data GROUP BY `time` ORDER BY `t` DESC''')
#     # 导出到Mysql
#     #  1.2 MySQL操作
#     create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_a_day", sql=''' create table `taobao_data_result`.`sql_a_day`  (
#   `brow_time` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `brow_num` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
#     ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#     ''')
#     #  1.3 Sqoop : Hive -> MySQL
#     hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_a_day", Hive_database="taobao_data1",Hive_tableName="a_day")
#
#
#     # 4. 用户行为（beha）
#     #  1.1 Hive操作
#     # 1.1创建Hive表
#     #######  需要进行判断，如果存在则跳过
#     create_table(host="localhost", database="taobao_data1", newTableName="beha",
#                  attributeList=''' `user_beha` varchar(255) , `num` varchar(255)''')
#     # 查询并插入
#     initUpdate_result_table(host="localhost", database="taobao_data1", TableName="beha",
#                      sql='''SELECT `behavior_type`,count(*) as `b` from data GROUP BY `behavior_type` ORDER BY `b` DESC''')
#     # 导出到Mysql
#     #  1.2 MySQL操作
#     create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_beha", sql=''' create table `taobao_data_result`.`sql_beha`  (
#   `user_beha` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `num` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
#     ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#     ''')
#     #  1.3 Sqoop : Hive -> MySQL
#     hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_beha", Hive_database="taobao_data1",Hive_tableName="beha")
#
#     # 6. 按照商品类别统计购买数量，查找前100条
#     #  1.1 Hive操作
#     # 1.1创建Hive表
#     #######  需要进行判断，如果存在则跳过
#     create_table(host="localhost", database="taobao_data1", newTableName="goods_buy",
#                  attributeList=''' `user_beha` varchar(255) , `num` varchar(255)''')
#     # 查询并插入
#     initUpdate_result_table(host="localhost", database="taobao_data1", TableName="goods_buy",
#                      sql='''select item_category, nums from (select item_category, count(*) as nums from data where `behavior_type`=4 group by item_category) as item_category_sortd order by nums DESC limit 0, 100''')
#     # 导出到Mysql
#     #  1.2 MySQL操作
#     create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_goods_buy", sql=''' create table `taobao_data_result`.`sql_goods_buy`  (
#   `buy_num` int CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `goods_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#     ''')
#     #  1.3 Sqoop : Hive -> MySQL
#     hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_goods_buy", Hive_database="taobao_data1",Hive_tableName="goods_buy")
#
#
#     # 6. 不同时间的购买量（buyplus_buy）
#     #  1.1 Hive操作
#     # 1.1创建Hive表
#     #######  需要进行判断，如果存在则跳过
#     create_table(host="localhost", database="taobao_data1", newTableName="buyplus_buy",
#                  attributeList=''' `goods_id` varchar(255) , `buyplus_num` varchar(255),`buy_num` varchar(255)''')
#     # 查询并插入
#     initUpdate_result_table(host="localhost", database="taobao_data1", TableName="buyplus_buy",
#                      sql='''select cart.item_category, cart.nums, purchasing.nums from (select item_category, nums from (select item_category, count(*) as nums from data where `behavior_type`=3 group by item_category) as item_category_sortd order by nums DESC limit 0, 100) as cart full join (select item_category, nums from (select item_category, count(*) as nums from data where `behavior_type`=4 group by item_category) as item_category_sortd order by nums DESC limit 0, 100) as purchasing on cart.item_category = purchasing.item_category order by cart.nums DESC''')
#     # 导出到Mysql
#     #  1.2 MySQL操作
#     create_mysql_table(host="localhost", user="root", passwd="root", port=3306, db="taobao_data_result", newtableName="sql_buyplus_buy", sql=''' create table `taobao_data_result`.`sql_buyplus_buy`  (
#   `goods_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `buyplus_num` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `buy_num` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
#     ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#     ''')
#     #  1.3 Sqoop : Hive -> MySQL
#     hive_to_mysql(host="localhost",portStr="3306",portInt=3306,user="root",passwd="root",MySQL_database="taobao_data_result",MySQL_tableName="sql_buyplus_buy", Hive_database="taobao_data1",Hive_tableName="buyplus_buy")
#
#




def updateHiveTables():
    # 0. 更新源数据
    overwriteUpdate_data_table(host="localhost", database="metro_hangzhou",
                               localDataPath="/root/Metro_Hangzhou/Metro_train_m/m_record_2019-01-01.csv", dataTableName="record_0101")

    # 5. N总数
    #  5.1 Hive操作

    # 首先清空
    # truncate_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums")
    #  # （1）总站点数
    # singleInsert_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums",
    #                              sql="select count(DISTINCT stationID ) from record_0101")
    #  # （2）总设备数
    # singleInsert_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums",
    #                              sql="select count(DISTINCT deviceID) from record_0101")
    #  # （3）总用户量
    # singleInsert_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums",
    #                              sql="select count(DISTINCT userID) from record_0101")
    #  # （4）总人次
    # singleInsert_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums",
    #                              sql="select count(DISTINCT time) from record_0101")
    # overwrite_Four_Update_result_table(host="localhost", database="metro_hangzhou", TableName="four_total_Nums",sql="")
    # # 导出到Mysql
    # #  5.2 MySQL操作
    # create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
    #                    newtableName="outputStation_Nums", sql=''' create table `metro_hangzhou`.`four_total_Nums`  (
    #   `type_Counts` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
    # ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic''')
    # #  5.3 Sqoop : Hive -> MySQL
    # print("updateHiveTables-hive_to_mysql")
    # hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop",
    #               MySQL_database="metro_hangzhou",
    #               MySQL_tableName="four_total_Nums", Hive_database="metro_hangzhou", Hive_tableName="four_total_nums")



    # 2.  进站人次排行（站点压力情况）柱状图
    #  2.1 Hive操作

    # 查询并更新
    overwriteUpdate_result_table(host="localhost", database="metro_hangzhou", TableName="inputStation_Nums",
                            sql="select stationID,count(*) as nums from record_0101  where status='1'  group by stationID  ORDER BY nums DESC")
    # 导出到Mysql
    #  2.2 MySQL操作
    create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
                       newtableName="inputStation_Nums", sql=''' create table `metro_hangzhou`.`inputStation_Nums`  (
      `stationID` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
      `inputNums` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic''')
    #  2.3 Sqoop : Hive -> MySQL
    print("updateHiveTables-hive_to_mysql")
    hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop", MySQL_database="metro_hangzhou",
        MySQL_tableName="inputStation_Nums", Hive_database="metro_hangzhou", Hive_tableName="inputstation_nums")


    # 3.  出站人次排行（从哪下车，哪里好玩）柱状图
    #  3.1 Hive操作

    # 查询并更新
    overwriteUpdate_result_table(host="localhost", database="metro_hangzhou", TableName="outputStation_Nums",
                            sql="select stationID,count(*) as nums from record_0101  where status='0'  group by stationID  ORDER BY nums DESC")
    # 导出到Mysql
    #  3.2 MySQL操作
    create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
                       newtableName="outputStation_Nums", sql=''' create table `metro_hangzhou`.`outputStation_Nums`  (
      `stationID` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
      `inputNums` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic''')
    #  3.3 Sqoop : Hive -> MySQL
    print("updateHiveTables-hive_to_mysql")
    hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop", MySQL_database="metro_hangzhou",
        MySQL_tableName="outputStation_Nums", Hive_database="metro_hangzhou", Hive_tableName="outputstation_nums")

    # 4.  出行选择线路（哪条线更受欢迎）饼图
    #  4.1 Hive操作

    # 查询并更新
    overwriteUpdate_result_table(host="localhost", database="metro_hangzhou", TableName="lineID_Nums",
                            sql="select lineID,count(*) from record_0101 group by lineID ORDER BY lineID")
    # 导出到Mysql
    #  4.2 MySQL操作
    create_mysql_table(host='localhost', user='hadoop', passwd='hadoop', port=3306, db='metro_hangzhou',
                       newtableName="outputStation_Nums", sql=''' create table `metro_hangzhou`.`lineID_Nums`  (
  `lineID` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `counts` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic''')
    #  4.3 Sqoop : Hive -> MySQL
    print("updateHiveTables-hive_to_mysql")
    hive_to_mysql(host="localhost", portStr="3306", portInt=3306, user="hadoop", passwd="hadoop", MySQL_database="metro_hangzhou",
        MySQL_tableName="lineID_Nums", Hive_database="metro_hangzhou", Hive_tableName="lineid_nums")





    return


def main():
    # os.system("conda activate base")
    # initHiveTables()
    while (1):
        # time.sleep(5)
        # os.system("python /home/hadoop/TmallBigdataAnalysis/display_pyecharts_refine.py")
        print("Begin updateHiveTables")
        updateHiveTables()
        break



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
    # initHiveTables()
    # updateHiveTables()
