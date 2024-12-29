import glob
import pandas as pd

def inter(a,b):  # 判断是否相交
    return list(set(a)&set(b))

def extract_track_files(path):  # 抽取轨迹实习材料的原始轨迹文件夹中的txt文件
    files = [filename for filename in glob.glob(path + '\*.txt')]#遍历一个目录下面所有的存放轨迹的txt文件
    return files

def get_track_points(filename):  # 读取轨迹文件中的轨迹点
    lines = []  # 存放单个轨迹文件中的所有轨迹点，由于每行中的字段可能并不完整，无法直接对齐生成Data Frame
    with open(filename, 'rb') as f: # 这里先用二进制读入，下面再解码，比文本读入速度更快
        for fLine in f:
            fLine = fLine.decode(encoding='mbcs').strip()# 每行最后有一个换行符，需要去掉
            line = [item for item in fLine.split(',')]# 按照逗号分割字段
            lines.append(line)
    f.close()
    return lines

def align_points(lines):  # 提取分析需要的字段，对齐轨迹点，并生成Data Frame
    # 原始轨迹数据中包含的异常内容大致如下所示，若发现新的异常值，可进行补充
    anomalies = ['超速报警', '补传', '定位故障', 'LED故障', '摄像头故障', '未定位', '溜车报警', '计价器故障', '紧急报警', '超速', 'ACC关']
    df = pd.DataFrame()  # 新建一个数据框
    points = [line[:7] for line in lines if not inter(line,anomalies)]# 清理、对齐轨迹点
    df = df.append(points)
    df.columns = ['出租车ID', '定位时间', '经度', '纬度', '方向', '速度', '空车/重车']  # 本次实习需要用到字段说明表里的前7个字段
    return df

# 完成轨迹点的文件读取
# 需要对应将dir换成测试文件所在目录
dir='D:\空间数据分析实习\任务1\data\raw'
files=extract_track_files(dir)
# 数据量较大，首先以其中一个文件为例
track_points = get_track_points(files[0])
df=align_points(track_points)

#完成轨迹点所需字段的切片，并通过数据表输出展示：
print(df.head(10))# 按时间排序
def sort_points(df):
    # 按条件排序
    df.sort_values(by=['出租车ID','定位时间'], inplace=True)
    # 重新生成索引
    df=df.reset_index(drop=True)

    # 在上下车时刻，数据里会有两条仅 空车/重车 字段不同的记录，形如
    # 100 2018-11-05 00:00:00 -- -- -- -- 空车
    # 100 2018-11-05 00:00:00 -- -- -- -- 重车
    # 经过上述排序后可能造成该字段的顺序出错，需要整理
    old_index=None
    old_row=None
    for index, row in df.iterrows():
        if old_index == None:
            old_index=index
            old_row=row
            continue
        if (row['出租车ID']==old_row['出租车ID'] and
            row['定位时间']==old_row['定位时间'] and 
            row['经度']==old_row['经度'] and 
            row['纬度']==old_row['纬度'] and 
            row['方向']==old_row['方向'] and 
            row['速度']==old_row['速度'] and 
            row['空车/重车']!=old_row['空车/重车']):
            #判断是否出现误排
            #看更前一行的状态
            older_row=df.iloc[old_index-1]
            if older_row['出租车ID']==row['出租车ID'] and older_row['空车/重车']==row['空车/重车']:
                # 交换，这并不会影响iterrows迭代器
                temp=df.iloc[index]
                df.iloc[index]=df.iloc[old_index]
                df.iloc[old_index]=temp
        old_index=index
        old_row=row

        return df

df=sort_points(df)
# 保存，便于后续反复读取
df.to_csv('D:\空间数据分析实习\任务1\data\processed\processed.txt',index=False)
# 由于数据较大，上述处理太慢，便于cell执行可从此处开始
import pandas as pd

# 读取
df=pd.read_csv('D:\空间数据分析实习\任务1\data\processed\processed.txt')
print(df.head(10))
# 对某辆出租车轨迹进行分段，提取上下车点
def get_sub_trajectory(df1):
    loads=[]
    no_loads=[]
    on_board=[]
    pick_up=[]
    drop_off=[]
    # 辅助记录
    idx1=-1 #记录每一段轨迹的开始
    idx2=-1 #记录每一段轨迹的结束
    old_status=''
    for index, row in df1.iterrows():
        status=row['空车/重车']
        # 初始化
        if index==0:
            idx1=index
            old_status=status
        # 判断状态是否转变
        # 记录状态改变的行索引
        if status!=old_status:
            sub_df=df1[idx1:idx2+1]
            if old_status=='重车':
                loads.append(sub_df)
                drop_off.append((row['经度'],row['纬度']))
            else:
                no_loads.append(sub_df)
                pick_up.append((row['经度'],row['纬度']))
            idx1=index
            idx2=index
            old_status=status
        else:
            idx2=index
    sub_df=df1[idx1:idx2+1]
    if old_status=='重车':
        loads.append(sub_df)
    else:
        no_loads.append(sub_df)
    return loads,no_loads,pick_up,drop_off

import geopandas as gpd
import matplotlib.pyplot as plt

# 选取某一辆出租车作为示例进行处理
df1=df[df['出租车ID']==1015]
# 构造GeoDataFrame对象
gdf1 = gpd.GeoDataFrame(
    df1, geometry=gpd.points_from_xy(df1['经度'], df1['纬度']),crs=4326)
# 使matplotlib支持中文字体
plt.rcParams['font.family']=['SimHei']
# 绘图，分别指定渲染字段、颜色表、显示图例、点大小、图片大小
ax=gdf1.plot(column='空车/重车',cmap='coolwarm',legend=True,markersize=1,figsize=(15,15))
# 指定范围
ax.set_ylim([30.4,30.8])
ax.set_xlim([114.0,114.6])
# 叠加武汉市路网
road = gpd.GeoDataFrame.from_file('D:\空间数据分析实习\任务1\data\osmWHmainRoad.shp')
road.plot(ax=ax,linewidth=0.5,alpha=0.5,color='grey')
import time,datetime
import math

from pyproj import Transformer
transformer1 = Transformer.from_crs(4326, 32649)
transformer2 = Transformer.from_crs(32649, 4326)

def interpolation(trac):
    trac_inter=pd.DataFrame()
    # 对太少的轨迹进行剔除
    if len(trac)<10:
        return trac_inter
    time1=datetime.datetime.strptime(trac.loc[0,'定位时间'], "%Y-%m-%d %H:%M:%S")
    for index, row in trac.iterrows():
        trac_inter=trac_inter.append(row,ignore_index=True)
        t1=datetime.datetime.strptime(row['定位时间'], "%Y-%m-%d %H:%M:%S")
        time_delta=(t1-time1).seconds
        # 180s内无数据，则干脆抛弃这条轨迹
        if time_delta>180:
            return pd.DataFrame()
        # 30s内无数据，则插值
        if time_delta>30:
            # 以15s间隔插值
            new_points=pd.DataFrame()
            n=int(time_delta/15)
            for i in range(n):
                time1=(time1+datetime.timedelta(seconds=15))
                row['定位时间']=time1.strftime("%Y-%m-%d %H:%M:%S")
                utm_x, utm_y = transformer1.transform(row['纬度'],row['经度'])
                delta_d=float(row['速度'])*1000/60/4
                delta_y=-math.sin(delta_d)
                delta_x=math.cos(delta_d)
                lat,lng=transformer2.transform(utm_x+delta_x,utm_y+delta_y)
                row['纬度']=lat
                row['经度']=lng
                trac_inter=trac_inter.append(row,ignore_index=True)
        time1=t1
    return trac_inter

def mean_filter(trac,n):
    # 窗口大小应该是奇数
    n=int(n/2)*2+1
    # 判断窗口大小
    if len(trac)<n:
        return pd.DataFrame()
    trac_filter=trac
    for i in range(int(n/2),len(trac)-int(n/2)):
        # 对于中间的点，进行均值滤波
        sub_df=trac[i-int(n/2):i+int(n/2)+1]
        lat=0.0
        lng=0.0
        for index, row in sub_df.iterrows():
            lat+=float(row['纬度'])
            lng+=float(row['经度'])
        lat/=n
        lng/=n
        # 将结果重新赋值
        trac_filter.loc[i,'纬度']=lat
        trac_filter.loc[i,'经度']=lng
    return trac_filter

# 对于某一条出租车轨迹
df1=df[df['出租车ID']==1015]
df1=df1.reset_index(drop=True)
# 首先提取所有轨迹段
loads,no_loads,pick_up,drop_off=get_sub_trajectory(df1)

new_loads=pd.DataFrame()
new_no_loads=pd.DataFrame()

# 对于所有的轨迹片段，分别进行滤波
n=5
for trac in loads:
    trac=trac.reset_index(drop=True)
    trac=interpolation(trac)
    trac_filter=mean_filter(trac,n)
    new_loads=new_loads.append(trac_filter,ignore_index=True)
for trac in no_loads:
    trac=trac.reset_index(drop=True)
    trac=interpolation(trac)
    trac_filter=mean_filter(trac,n)
    new_no_loads=new_no_loads.append(trac_filter,ignore_index=True)

# 汇总滤波之后的轨迹
new_df=new_loads.append(new_no_loads)

# 可视化查看
gdf = gpd.GeoDataFrame(
    new_df, geometry=gpd.points_from_xy(new_df['经度'], new_df['纬度']))
ax=gdf.plot(column='空车/重车',cmap='coolwarm',legend=True,markersize=1,figsize=(15,15))
ax.set_ylim([30.4,30.8])
ax.set_xlim([114.0,114.6])
# 叠加武汉市路网
road = gpd.GeoDataFrame.from_file('D:\空间数据分析实习\任务1\data\osmWHmainRoad.shp')
road.plot(ax=ax,linewidth=0.5,alpha=0.5,color='grey')

from shapely.geometry import LineString,Point
from shapely.ops import nearest_points
import pyproj

# 投影相关
wgs84=pyproj.CRS.from_epsg(4326)
utm49N=pyproj.CRS.from_epsg(32649)
myTrans=pyproj.Transformer.from_crs(utm49N,wgs84)

def mapping(trac,radius,road):
    mapped=trac
    # 点转线
    if len(trac)==0:
        return pd.DataFrame()
    xylist=[xy for xy in zip(trac['经度'],trac['纬度'])]
    line=LineString(xylist)
    gdf = gpd.GeoDataFrame(geometry = [line],crs=4326)
    # WGS84 转 UTM zone 49N:
    gdf = gdf.to_crs(32649)
    # 线缓冲区
    gs_buffer=gdf.buffer(radius)
    gs_buffer=gs_buffer.to_crs(4326)
    gdf_buffer=gpd.GeoDataFrame(geometry=gs_buffer)
    # 与路网求交
    subroad = gpd.overlay(road,gdf_buffer, how = 'intersection').to_crs(32649)
    # 遍历轨迹点，计算距离
    if subroad.geometry.unary_union.is_empty:
        return mapped
    for index, row in trac.iterrows():
        pt = gpd.GeoSeries([Point(row['经度'],row['纬度'])],crs=4326).to_crs(32649)
        # 使用shapely的nearest_points方法计算点到线上的最近点，该点即为匹配点
        npts= nearest_points(pt[0],subroad.geometry.unary_union)
        npt=npts[1]
        # 注意投影变换
        npt=myTrans.transform(npt.x,npt.y)
        mapped.loc[index,'经度']=npt[1]
        mapped.loc[index,'纬度']=npt[0]
    return mapped

# 对于某一条出租车轨迹
df1=df[df['出租车ID']==1015]
df1=df1.reset_index(drop=True)
# 首先提取所有轨迹段
loads,no_loads,pick_up,drop_off=get_sub_trajectory(df1)

new_df=pd.DataFrame()

# 对于所有的轨迹片段，分别进行匹配
dis=10
for trac in no_loads:
    trac=trac.reset_index(drop=True)
    trac=interpolation(trac)
    trac_mapped=mapping(trac,dis,road)
    new_df=new_df.append(trac_mapped,ignore_index=True)
for trac in loads:
    trac=trac.reset_index(drop=True)
    trac=interpolation(trac)
    trac_mapped=mapping(trac,dis,road)
    new_df=new_df.append(trac_mapped,ignore_index=True)

# 可视化查看
gdf = gpd.GeoDataFrame(
    new_df, geometry=gpd.points_from_xy(new_df['经度'], new_df['纬度']))
ax=gdf.plot(column='空车/重车',cmap='coolwarm',legend=True,markersize=1,figsize=(15,15))
ax.set_ylim([30.4,30.8])
ax.set_xlim([114.0,114.6])
# 叠加武汉市路网
road = gpd.GeoDataFrame.from_file('D:\空间数据分析实习\任务1\data\osmWHmainRoad.shp')
road.plot(ax=ax,linewidth=0.5,alpha=0.5,color='grey')