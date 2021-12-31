# coding=UTF-8
import pymysql
from pyecharts import options as opts
from pyecharts.charts import Pie, Page, Bar, Line, Map, Radar
from pyecharts.globals import ChartType, SymbolType, ThemeType
import pandas as pd
import numpy as np

page = Page(layout=Page.DraggablePageLayout, page_title="天猫数据分析")
conn = pymysql.connect(host='localhost', user='root', passwd='root', port=3306, db='taobao_data_cjj', charset='utf8')
cur = conn.cursor();
prov = "SELECT province,goods_type FROM sql_goods_c "
cur.execute(prov)
provs = cur.fetchall()


# 1111111111111111111111111111
def map_china() -> Map:
    d_map = (
        Map(init_opts=opts.InitOpts(theme="chalk"))
            .add(series_name="各省份的商品种类", data_pair=provs, maptype="china", zoom=1, center=[105, 38])
            .set_global_opts(
            title_opts=opts.TitleOpts(title="商品种类数分布省份"),
            visualmap_opts=opts.VisualMapOpts(max_=4100, is_piecewise=True,
                                              pieces=[
                                                  {"max": 4060, "min": 4041, "label": "4041-4060", "color": "#fff5a5"},
                                                  {"max": 4080, "min": 4061, "label": "4061-4080", "color": "#f0bbe1"},
                                                  {"max": 4100, "min": 4081, "label": "4081-4100", "color": "#ff88a1"},
                                                  {"max": 4120, "min": 4101, "label": "4101-4120", "color": "#ffe001"},
                                                  {"max": 4140, "min": 4121, "label": "4021-4040", "color": "#f0ae30"},
                                                  {"max": 4160, "min": 4141, "label": "4041-4060", "color": "#ff700f"},
                                                  {"max": 4180, "min": 4161, "label": "4161-4080", "color": "#ff1130"},
                                                  {"max": 4200, "min": 4181, "label": ">=4200", "color": "#efaaff"}]

                                              )
        )
    )
    return d_map


d_map = map_china()
d_map.render(path="/home/hadoop/display/result/商品.html")
page.add(d_map)
# 2222222222222222222
# 统计各省份总访问量柱状图
# 排序取前10条记录
visit_pv = "SELECT province,brow_num FROM sql_visit_PV  ORDER BY brow_num DESC limit 10"
cur.execute(visit_pv)
vs_pv = cur.fetchall()
# 统计各省访客数
# 排序取前10条记录
visit_uv = " SELECT province,brow_num FROM sql_visit_UV  ORDER BY brow_num DESC limit 10 "
cur.execute(visit_uv)
vs_uv = cur.fetchall()
x = [x[0] for x in vs_pv]
y1 = [x[1] for x in vs_pv]
y2 = [x[1] for x in vs_uv]


def bar() -> Pie:
    bar = (
        Bar(
            init_opts=opts.InitOpts(
                animation_opts=opts.AnimationOpts(
                    animation_delay=1000, animation_easing="bounceIn"
                ), theme="chalk"
            )
        )
            .add_xaxis(x)
            .add_yaxis('浏览量', y1)
            .add_yaxis('访客量', y2, category_gap='50%')
            #       .set_colors(["black",'red'])
            .set_global_opts(title_opts=opts.TitleOpts(title='访客量和浏览量排前10省份'),
                             yaxis_opts=opts.AxisOpts(name="浏览量和访客数", min_=93600),
                             xaxis_opts=opts.AxisOpts(name="省份"),
                             )

    )
    return bar


bar = bar()
bar.render("/home/hadoop/display/result/pv_uv量柱状图.html")
page.add(bar)
# 333333333333333333333333333
# 购买量前10的商品种类-雷达图
# gbs= "SELECT goods_id,buy_num FROM sql_goods_buy  ORDER BY 购买数量 DESC limit 10"
gbs = "SELECT goods_id,buy_num FROM sql_goods_buy  ORDER BY buy_num DESC"
cur.execute(gbs)
gb = cur.fetchall()
print(gb)
name = [x[0] for x in gb]
num = [x[1] for x in gb]

sum1 = 0
for i in range(len(num)):
    sum1 = sum1 + (int)(num[i])
avg = sum1 / len(num)
avg = int(avg)
avg1 = []
for i in range(10):
    avg1.append(avg)
num1 = num[:10]
print(num1)


def radar_base() -> Radar:
    c = (
        Radar(init_opts=opts.InitOpts(page_title="雷达图-演示", theme="chalk"))
            .add_schema(
            schema=[
                opts.RadarIndicatorItem(name="口红", max_=15000),
                opts.RadarIndicatorItem(name="粉底液", max_=15000),
                opts.RadarIndicatorItem(name="手机", max_=15000),
                opts.RadarIndicatorItem(name="笔记本", max_=15000),
                opts.RadarIndicatorItem(name="电视", max_=15000),
                opts.RadarIndicatorItem(name="饮料", max_=15000),
                opts.RadarIndicatorItem(name="服饰", max_=15000),
                opts.RadarIndicatorItem(name="保温杯", max_=15000),
                opts.RadarIndicatorItem(name="口罩", max_=15000),
                opts.RadarIndicatorItem(name="奶粉", max_=15000)
            ],
            shape="circle"
        )
            .add(
            "购买数量",
            [num1],
            areastyle_opts=opts.AreaStyleOpts(opacity=0.2)
        )
            .add(
            "总购买平均",
            [avg1],
            linestyle_opts=opts.LineStyleOpts(width=2, color="#0ef0c8")

        )
            .set_global_opts(
            title_opts=opts.TitleOpts(title="购买数量前10对应的商品", pos_left="center"),
            legend_opts=opts.LegendOpts(pos_right="20%", pos_top="10%", orient="vectical")
        )

    )
    return c


c = radar_base()
c.render("/home/hadoop/display/result/雷达图.html")
page.add(c)  # 加入page

# 每日访问量折线图
time = "SELECT date_format(brow_time,'%m-%d') ,brow_num FROM sql_a_day ORDER BY  date_format(brow_time,'%m-%d');"
cur.execute(time)
time1 = cur.fetchall()
xx = [x[0] for x in time1]
yy = [x[1] for x in time1]


def line() -> Line:
    line = (
        Line(init_opts=opts.InitOpts(theme="chalk"))
            .add_xaxis(xx)
            .add_yaxis('2014年每日访问量', yy)

            .set_global_opts(title_opts=opts.TitleOpts(title="每日访问量", subtitle="11-12月每日访问量"),
                             yaxis_opts=opts.AxisOpts(min_=80000))

    )
    return line


line = line()
line.render("/home/hadoop/display/result/2014年11-12月每日访问量折线图.html")
page.add(line)

# 用户行为饼图
nums = "SELECT  num FROM sql_beha "
cur.execute(nums)
num = cur.fetchall()
numlist = []
numlist = [x[0] for x in num]
beha = ['浏览', '收藏', '加购物车', '购买']


def pie() -> Pie:
    pie = (
        Pie(init_opts=opts.InitOpts(theme="chalk"))
            .add(
            "各省份销售量",
            [list(z) for z in zip(beha, numlist)], radius=["50%", "75%"],  # 圆环的粗细和大小
        )
            .set_global_opts(title_opts=opts.TitleOpts(title="用户行为"))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"),
                             toolbox_opts=opts.ToolboxOpts()
                             )

    )
    return pie


pie = pie()
pie.render("/home/hadoop/display/result/用户行为饼图.html")
page.add(pie)

# page.render("/home/hadoop/display/result/总.html")
Page.save_resize_html("/home/hadoop/display/result/总.html",
                      cfg_file="/home/hadoop/display/result/chart_config.json",
                      dest="/home/hadoop/display/result/my_test.html")