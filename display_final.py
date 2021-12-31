# 1.导入包
import pymysql
from pyecharts import options as opts
from pyecharts.charts import *
from pyecharts.globals import ChartType, SymbolType,ThemeType
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
page = Page(layout=Page.DraggablePageLayout,page_title= "天猫数据分析")

conn = pymysql.connect(host='localhost',user='root',passwd='123456789',port=3306,db='mysql',charset='utf8')
cur = conn.cursor();
prov= "SELECT 省份,商品种类数 FROM goods_c "
cur.execute(prov)
provs= cur.fetchall()
#1111111111111111111111111111
def map_china() -> Map:
    d_map= (
    Map(init_opts=opts.InitOpts(theme="dark"))#purple-passion"))
    .add(series_name="各省份的商品种类", data_pair=provs, maptype="china",zoom = 1,center=[105,38])
      .set_global_opts(  
      title_opts=opts.TitleOpts(title="商品种类数分布省份"),
      visualmap_opts=opts.VisualMapOpts(max_=4100,is_piecewise=True,
              pieces=[
                  {"max": 4060, "min": 4041, "label": "4041-4060","color":"#fff5a5"},
                 {"max": 4080, "min": 4061, "label": "4061-4080","color":"#f0bbe1"},
                  {"max": 4100, "min": 4081, "label": "4081-4100","color":"#ff88a1"},
                  {"max": 4120, "min": 4101, "label": "4101-4120","color":"#ffe001"},
                  {"max": 4140, "min": 4121, "label": "4021-4040","color":"#f0ae30"},
                  {"max": 4160, "min": 4141, "label": "4041-4060","color":"#ff700f"},
                  {"max": 4180, "min": 4161, "label": "4161-4080","color":"#ff1130"},
                  {"max": 4200, "min": 4181, "label": ">=4200", "color":"#efaaff"}],
                                        
                       )
        )
        
    )
    
    return d_map
d_map = map_china()
d_map.render(path="D:\\python1\\dsj\\result\\商品.html")
page.add(d_map)
#2222222222222222222
#统计各省份总访问量柱状图
#排序取前10条记录
visit_pv= "SELECT 所在省份,浏览数量 FROM visit_pv  ORDER BY 浏览数量 DESC limit 10"
cur.execute(visit_pv)
vs_pv= cur.fetchall()
#统计各省访客数
#排序取前10条记录
visit_uv= " SELECT 所在省份,访客数 FROM visit_uv  ORDER BY 访客数 DESC limit 10 "
cur.execute(visit_uv)
vs_uv= cur.fetchall()
x= [x[0] for x in vs_pv]
y1 = [x[1] for x in vs_pv]
y2 = [x[1] for x in vs_uv]
def bar()->Bar:
    bar=(
      Bar(
          init_opts=opts.InitOpts(
          animation_opts=opts.AnimationOpts(
              animation_delay=1000,animation_easing="bounceIn"
          ),theme="chalk"
          )
      )
      .add_xaxis(x)
      .add_yaxis('浏览量',y1,color="#b54abe")
      .add_yaxis('访客量',y2,category_gap='50%')
#       .set_colors(["black",'red'])
      .reversal_axis()
    .set_series_opts(label_opts=opts.LabelOpts(position="right"))
      .set_global_opts(title_opts=opts.TitleOpts(title='访客量和浏览量排前10省份'),
                         xaxis_opts=opts.AxisOpts(name="浏览量和访客数",min_=93600),
                        yaxis_opts=opts.AxisOpts(name="省份"),
                             )
                                                    

      )
    return bar
bar=bar()
bar.render("D:\\python1\\dsj\\result\\pv_uv量柱状图.html")
page.add(bar)
#333333333333333333333333333
#购买量前10的商品种类-雷达图
# gbs= "SELECT 商品编号,购买数量 FROM goods_buy  ORDER BY 购买数量 DESC limit 10"
gbs= "SELECT 商品编号,购买数量 FROM goods_buy  ORDER BY 购买数量 DESC"
cur.execute(gbs)
gb= cur.fetchall()
name= [x[0] for x in gb]
num=[x[1] for x in gb]

sum1=0
for i in range(len(num)):
    sum1+=num[i]
avg=sum1/len(num)
avg=int(avg)
avg1=[]
for i in range(10):
    avg1.append(avg)
num1=num[:10]

def radar_base() -> Radar:
    c = (
        Radar(init_opts=opts.InitOpts(page_title="雷达图-演示",theme="dark"))
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
            linestyle_opts=opts.LineStyleOpts(width=2,color="#0ef0c8")

            )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="购买数量前10对应的商品",pos_left="center"),
            legend_opts=opts.LegendOpts(pos_left="0%", pos_top="8%", orient="vectical")
            )

    )
    return c
c=radar_base()
c.render("D:\\python1\\dsj\\result\\雷达图.html")
page.add(c)#加入page
###444444
#每日访问量折线图
time= "SELECT date_format(访问日期,'%m-%d') ,访问数量 FROM a_day ORDER BY  date_format(访问日期,'%m-%d');"
cur.execute(time)
time1= cur.fetchall()
xx= [x[0] for x in time1]
yy= [x[1] for x in time1]
def line()->Line:
    line = (
      Line(init_opts=opts.InitOpts(theme="dark"))
      .add_xaxis(xx)
      .add_yaxis('2014年每日访问量',yy)
      .set_global_opts(title_opts=opts.TitleOpts(title="每日访问量", subtitle="11-12月每日访问量",
                            title_textstyle_opts=opts.TextStyleOpts(color="#FFedFf")), 
                      yaxis_opts=opts.AxisOpts(min_=80000 ))

     )
    return line
line=line()
line.render("D:\\python1\\dsj\\result\\2014年11-12月每日访问量折线图.html")
page.add(line)

#5555555555555555
#用户行为饼图
nums= "SELECT  数量 FROM beha "
cur.execute(nums)
num= cur.fetchall()
numlist=[]
sum=0;
numlist= [x[0] for x in num]
for i in range(len(numlist)):
    sum=sum+numlist[i]
rate=(numlist[0]/sum)#只看不买，跳失率
print(rate)
beha=['浏览','收藏','加购物车','购买']
def pie()->Pie:
    pie = (
      Pie(init_opts=opts.InitOpts(theme="macarons"))#init_opts=opts.InitOpts(theme="chalk")
      .add(
        "各省份销售量",
      [list(z) for z in zip(beha,numlist)],radius=["50%", "75%"],   # 圆环的粗细和大小
     )
    .set_global_opts(title_opts=opts.TitleOpts(title="用户行为",pos_bottom='10%',
                        title_textstyle_opts=opts.TextStyleOpts(color="#FFedFf")))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}%"),
    toolbox_opts=opts.ToolboxOpts(),legend_opts=opts.LegendOpts(textstyle_opts=opts.TextStyleOpts(color="#FFFFFF"),
                                                                type_="scroll",orient="vertical",pos_right="5%",pos_top="middle")
     )
#      .set_colors(["red","Cyan" "green", "purple"])
   
  )
    return pie
pie=pie()
pie.render("D:\\python1\\dsj\\result\\用户行为饼图.html")
page.add(pie)
#66666666
#跳失率-水球

c = (
    Liquid() # 水球图
    .add("跳失率（只看不买）", [rate, 0.5], # "lq" 悬浮信息，前者是显示的数值，后者是上浮的面积
         )
    .set_global_opts(title_opts=opts.TitleOpts(title="跳失率（只看不买）",
            pos_left='center', pos_top=30,
          title_textstyle_opts=opts.TextStyleOpts(color="#FFedFf")))  #标题
)
page.add(c)
#####777777777
#大标题
big_title = (
    Pie()
        .set_global_opts(
        title_opts=opts.TitleOpts(title="   — 天猫数据分析 —",
                    title_textstyle_opts=opts.TextStyleOpts(font_size=30, color='#ffabFF',
                   border_radius=True, border_color="white"),
                                  pos_top=0)))
page.add(big_title)
###生成词云
# wc = (
#     WordCloud(init_opts=opts.InitOpts(theme="romatic"))
#     .add("省份",provs ,word_size_range=[10, 50])
# )
page.render("D:\\python1\\dsj\\result\\总.html")    
with open("D:\\python1\\dsj\\result\\总.html", "r+", encoding='utf-8') as html:
    html_bf = BeautifulSoup(html, 'lxml')
    divs = html_bf.select('.chart-container')
    divs[0][
        "style"] = "width:680px;height:370px;position:absolute;top:50px;left:310px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[1][
        "style"] = "width:600px;height:370px;position:absolute;top:420px;left:0px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[2][
        "style"] = "width:400px;height:330px;position:absolute;top:50px;left:940px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[3][
        'style'] = "width:580px;height:420px;position:absolute;top:370px;left:850px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[4][
        "style"] = "width:310px;height:350px;position:absolute;top:70px;left:0px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[5][
        "style"] = "width:400px;height:400px;position:absolute;top:420px;left:520px;border-style:solid;border-color:#444444;border-width:0px;"
    divs[6][
        'style'] = "width:400px;height:35px;position:absolute;top:10px;left:420px;border-style:solid;border-color:#444444;border-width:0px;"
  
    body = html_bf.find("body")
    body["style"] = "background-color:#333333;"
#     body["style"] = "background-image: url(bg4.jpg)"
    html_new = str(html_bf)
    html.seek(0, 0)
    html.truncate()
    html.write(html_new)
    html.close()
Page.save_resize_html("D:\\python1\\dsj\\result\\总.html",
                      cfg_file="D:\\Download\\chart_config.json",
                     dest="D:\\python1\\dsj\\result\\my_test.html")

