#!/usr/bin/python3
# -*- coding:utf-8 -*-

# @Time      :  2018/5/7 10:47
# @Auther    :  WangYang
# @Email     :  evilwangyang@126.com
# @Project   :  XDR
# @File      :  analysis.py
# @Software  :  PyCharm Community Edition

# ********************************************************* 
import pandas as pd
import os
from datetime import datetime,timedelta



#统计不同业务大类流量占比及业务应用流量占比
def OTT_ratio (xdr_path,result_path):
	if os.path.isdir(result_path + '\\不同OTT流量占比'):
		pass
	else:
		os.mkdir(result_path + '\\不同OTT流量占比')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	OTT_ratio_1 = A_file.groupby(['大类业务类型'],as_index=False)['总流量(GB)'].sum()
	OTT_ratio_1['全流量(GB)'] = sum(OTT_ratio_1['总流量(GB)'])
	OTT_ratio_1['流量占比(%)'] = OTT_ratio_1['总流量(GB)']/OTT_ratio_1['全流量(GB)']*100
	OTT_ratio_1.drop(['全流量(GB)'], axis=1, inplace=True)
	OTT_ratio_1 = OTT_ratio_1.sort_values(by=['总流量(GB)'],ascending=False)
	OTT_ratio_1 = OTT_ratio_1.round({'总流量(GB)':2,'流量占比(%)': 2})
	OTT_ratio_1.to_csv(result_path+'\\不同OTT流量占比'+'\\业务大类流量占比.csv',sep=',',index=False)
	OTT_ratio_2 = A_file.groupby(['大类业务类型','业务名称'], as_index=False)['总流量(MB)'].sum()
	OTT_ratio_2['全流量(MB)'] = sum(OTT_ratio_2['总流量(MB)'])
	OTT_ratio_2['流量占比(%)'] = OTT_ratio_2['总流量(MB)']/OTT_ratio_2['全流量(MB)']*100
	OTT_ratio_2.drop(['全流量(MB)'], axis=1, inplace=True)
	OTT_ratio_2 = OTT_ratio_2.sort_values(by=['总流量(MB)'], ascending=False)
	OTT_ratio_2 = OTT_ratio_2.round({'总流量(MB)':2,'流量占比(%)': 2})
	OTT_ratio_2.to_csv(result_path + '\\不同OTT流量占比' + '\\业务应用流量占比.csv', sep=',', index=False)
	A_name.close()


#统计整体及不同业务的用户数
def Video_user(xdr_path,result_path):
	if os.path.isdir(result_path + '\\视频用户数'):
		pass
	else:
		os.mkdir(result_path + '\\视频用户数')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')

	B_file = A_file.loc[:, ['IMSI', 'IMEI']]
	B_file = B_file.drop_duplicates(['IMSI', 'IMEI'])
	B_file.to_csv(result_path + '\\视频用户数' + '\\视频用户列表.csv', sep=',', index=False)

	C_file = A_file.loc[:, ['IMSI', 'IMEI','业务名称']]
	C_file = C_file.drop_duplicates(['IMSI', 'IMEI','业务名称'])
	C_file['用户数'] = 1
	C_file = C_file.groupby(['业务名称'], as_index=False)['用户数'].sum()
	C_file = C_file[C_file['业务名称'] != '未知业务']
	C_file = C_file.sort_values(by=['用户数'], ascending=True)
	C_file.to_csv(result_path + '\\视频用户数' + '\\不同业务用户数统计.csv', sep=',', index=False)
	A_name.close()


#data_period中用来选取时间的时分秒
def transform_HMS(x):
	return str(x).split()[1]

#统计不同时段视频流量占比
def data_period (xdr_path,result_path):
	if os.path.isdir(result_path + '\\不同时段流量占比'):
		pass
	else:
		os.mkdir(result_path + '\\不同时段流量占比')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	B_file = A_file.loc[:, ['开始时间', '总流量(MB)']]
	B_file = B_file.set_index('开始时间')
	B_file.index = pd.to_datetime(B_file.index)
	B_file = B_file.resample('10min').sum()
	B_file['开始时刻'] = B_file.index
	B_file['开始时刻'] = B_file['开始时刻'].map(transform_HMS)
	B_file['总流量(MB)'].fillna(0, inplace=True)
	B_file['全流量(MB)'] = sum(B_file['总流量(MB)'])
	B_file['流量占比(%)'] = B_file['总流量(MB)'] / B_file['全流量(MB)'] * 100
	B_file.drop(['全流量(MB)'], axis=1, inplace=True)
	B_file = B_file.sort_values(by=['开始时刻'], ascending=True)
	B_file = B_file.set_index('开始时刻')
	B_file = B_file.round({'总流量(MB)': 2, '流量占比(%)': 2})
	B_file.to_csv(result_path + '\\不同时段流量占比'+'\\不同时段流量占比.csv', sep=',')
	A_name.close()


#data_time_duration中用来对视频时长分段
def segmentation_duration(x):
	if x < timedelta(minutes=1):
		return '0-1分钟'
	elif x < timedelta(minutes=5):
		return '1-5分钟'
	elif x < timedelta(minutes=10):
		return '5-10分钟'
	elif x < timedelta(minutes=30):
		return '10-30分钟'
	elif x < timedelta(minutes=60):
		return '30-60分钟'
	elif x < timedelta(minutes=90):
		return '60-90分钟'
	else :
		return '90分钟以上'

#统计不同视频时长的流量占比及播放次数
def data_time_duration (xdr_path,result_path):
	if os.path.isdir(result_path + '\\不同长度视频流量及点击次数占比'):
		pass
	else:
		os.mkdir(result_path + '\\不同长度视频流量及点击次数占比')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	B_file = A_file.loc[:, ['开始时间', '结束时间', '总流量(MB)']]
	B_file['开始时间'] = pd.to_datetime(B_file['开始时间'])
	B_file['结束时间'] = pd.to_datetime(B_file['结束时间'])
	B_file = B_file[B_file['结束时间'] > B_file['开始时间']]
	B_file['视频时长'] = B_file['结束时间'] - B_file['开始时间']
	B_file['视频区间'] = B_file['视频时长'].map(segmentation_duration)
	B_file['播放次数'] = 1
	B_file = B_file.groupby(['视频区间'], as_index=False).agg({'总流量(MB)': 'sum', '播放次数': 'sum'})
	B_file['全流量(MB)'] = sum(B_file['总流量(MB)'])
	B_file['流量占比(%)'] = B_file['总流量(MB)'] / B_file['全流量(MB)'] * 100
	B_file['总播放次数'] = sum(B_file['播放次数'])
	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
	B_file = B_file.sort_values(by=['视频区间'], ascending=True)
	B_file.drop(['全流量(MB)','总播放次数'], axis=1, inplace=True)
	B_file = B_file.round(2)
	B_file.to_csv(result_path + '\\不同长度视频流量及点击次数占比' + '\\不同长度视频流量及点击次数占比.csv', sep=',',index=False)
	A_name.close()

#time_codeRate中用来对视频码率分段
def segmentation_codeRate(x):
	if x < 0.5:
		return '小于0.5Mbps'
	elif x < 1:
		return '0.5-1Mbps'
	elif x < 1.5:
		return '1-1.5Mbps'
	elif x < 2:
		return '1.5-2Mbps'
	elif x < 2.5:
		return '2-2.5Mbps'
	elif x < 5:
		return '2.5-5Mbps'
	elif x < 10:
		return '5-10Mbps'
	else :
		return '10Mbps以上'


#统计不同码率视频的播放次数占比
def time_codeRate(xdr_path,result_path):
	if os.path.isdir(result_path + '\\不同码率视频播放次数占比'):
		pass
	else:
		os.mkdir(result_path + '\\不同码率视频播放次数占比')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['流媒体码率(Mbps)'] = A_file['流媒体码率(kbps)'] / 1000

	B_file = A_file.loc[:, ['流媒体码率(Mbps)']]
	B_file['码率区间'] = B_file['流媒体码率(Mbps)'].map(segmentation_codeRate)
	B_file['播放次数'] = 1
	B_file = B_file.groupby(['码率区间'], as_index=False)['播放次数'].sum()
	B_file['总播放次数'] = sum(B_file['播放次数'])
	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
	B_file = B_file.sort_values(by=['码率区间'], ascending=True)
	B_file.drop(['总播放次数'], axis=1, inplace=True)
	B_file = B_file.round({'播放次数占比(%)':2})
	B_file.to_csv(result_path + '\\不同码率视频播放次数占比' + '\\不同码率视频播放次数占比.csv', sep=',', index=False)
	A_name.close()


#Video_play_SuccessRate中用来判断视频播放是否成功
def success_or_not(x,y):
	if x/1000>y*2/8:#x为实际下载的视频流媒体字节数(字节)，y为流媒体码率(kbps)，当前视频格式多为2s缓冲量则可播放，故y*2为播放所需缓冲的视频大小
		return 1
	else :
		return 0

#统计不同维度的视频播放成功率
#当实际下载的视频流媒体字节数大于视频播放所需缓冲的视频流量大小时，认为视频播放成功
def Video_play_SuccessRate(xdr_path,result_path):
	if os.path.isdir(result_path + '\\视频播放成功率'):
		pass
	else:
		os.mkdir(result_path + '\\视频播放成功率')
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv'
	A_name = open(file_xdr, encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['成功次数'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
	A_file['播放次数'] = 1

	B_file = A_file.loc[:, ['成功次数', '播放次数']]
	B_file.loc['sum'] = B_file.apply(lambda x: x.sum())
	B_file = B_file.loc[['sum'], ['成功次数', '播放次数']]
	B_file['播放成功率(%)'] = B_file['成功次数'] / B_file['播放次数'] * 100
	B_file = B_file.round({'播放成功率(%)': 2})
	B_file.to_csv(result_path + '\\视频播放成功率' + '\\整体视频播放成功率.csv', sep=',', index=False)

	C_file = A_file.loc[:, ['开始时间', '成功次数', '播放次数']]
	C_file = C_file.set_index('开始时间')
	C_file.index = pd.to_datetime(C_file.index)
	C_file = C_file.resample('10min').sum()
	C_file['开始时刻'] = C_file.index
	C_file['开始时刻'] = C_file['开始时刻'].map(transform_HMS)
	C_file = C_file.groupby(['开始时刻'], as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	C_file['播放成功率(%)'] = C_file['成功次数'] / C_file['播放次数'] * 100
	C_file = C_file.sort_values(by=['开始时刻'], ascending=True)
	C_file = C_file.round({'播放成功率(%)': 2})
	C_file.to_csv(result_path + '\\视频播放成功率' + '\\不同时刻视频播放成功率.csv', sep=',', index=False)

	D_file = A_file.loc[:, ['业务名称', '成功次数', '播放次数']]
	D_file = D_file.groupby(['业务名称'], as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	D_file['播放成功率(%)'] = D_file['成功次数'] / D_file['播放次数'] * 100
	D_file = D_file[(D_file['播放次数'] > 100) & (D_file['播放成功率(%)'] < 95) & (D_file['业务名称'] != '未知业务')]
	D_file = D_file.sort_values(by=['播放成功率(%)'], ascending=True)
	D_file = D_file.round({'播放成功率(%)': 2})
	D_file.to_csv(result_path + '\\视频播放成功率' + '\\视频播放成功率低的业务列表.csv', sep=',', index=False)

	E_file = A_file.loc[:, ['业务名称','访问IP', '成功次数', '播放次数']]
	E_file = E_file.groupby(['业务名称','访问IP'], as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	E_file['播放成功率(%)'] = E_file['成功次数'] / E_file['播放次数'] * 100
	E_file = E_file[(E_file['播放次数'] > 10) & (E_file['播放成功率(%)'] < 95)& (E_file['业务名称'] != '未知业务')]
	E_file = E_file.sort_values(by=['业务名称','播放成功率(%)'], ascending=True)
	E_file = E_file.round({'播放成功率(%)': 2})
	E_file.to_csv(result_path + '\\视频播放成功率' + '\\视频播放成功率低的服务器IP地址列表.csv', sep=',', index=False)

	F_file = A_file.loc[:, ['地市', '场景', '基站名称', '基站ID', '小区名称', '小区ID', '小区经度/RRU经度', '小区纬度/RRU纬度', '成功次数', '播放次数']]
	F_file = F_file.groupby(['地市', '场景', '基站名称', '基站ID', '小区名称', '小区ID', '小区经度/RRU经度', '小区纬度/RRU纬度'],
	                        as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	F_file['播放成功率(%)'] = F_file['成功次数'] / F_file['播放次数'] * 100
	F_file = F_file[(F_file['播放次数'] > 10) & (F_file['播放成功率(%)'] < 95)]
	F_file = F_file.sort_values(by=['播放成功率(%)'], ascending=True)
	F_file = F_file.round({'播放成功率(%)': 2})
	F_file.to_csv(result_path + '\\视频播放成功率' + '\\视频播放成功率低的问题小区列表.csv', sep=',', index=False)

	G_file = A_file.loc[:, ['IMSI','IMEI','品牌','型号', '成功次数', '播放次数']]
	G_file = G_file.groupby(['IMSI','IMEI','品牌','型号'], as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	G_file['播放成功率(%)'] = G_file['成功次数'] / G_file['播放次数'] * 100
	G_file = G_file[(G_file['播放次数'] > 10) & (G_file['播放成功率(%)'] < 95)]
	G_file = G_file.sort_values(by=['播放成功率(%)'], ascending=True)
	G_file = G_file.round({'播放成功率(%)': 2})
	G_file.to_csv(result_path + '\\视频播放成功率' + '\\视频播放成功率低的用户列表.csv', sep=',', index=False)
	
	H_file = A_file.loc[:, ['品牌','型号', '成功次数', '播放次数']]
	H_file = H_file.groupby(['品牌','型号'], as_index=False).agg({'成功次数': 'sum', '播放次数': 'sum'})
	H_file['播放成功率(%)'] = H_file['成功次数'] / H_file['播放次数'] * 100
	H_file = H_file[(H_file['播放次数'] > 10) & (H_file['播放成功率(%)'] < 95)& (H_file['型号'] != '未知型号')]
	H_file = H_file.sort_values(by=['播放成功率(%)'], ascending=True)
	H_file = H_file.round({'播放成功率(%)': 2})
	H_file.to_csv(result_path + '\\视频播放成功率' + '\\视频播放成功率低的终端列表.csv', sep=',', index=False)

	A_name.close()


#Video_play_Delay中用来对视频播放等待时长分段
def segmentation_play_Delay(x):
	if x < 10:
		return '小于10ms'
	elif x < 50:
		return '10-50ms'
	elif x < 100:
		return '50-100ms'
	elif x < 200:
		return '100-200ms'
	elif x < 500:
		return '200-500ms'
	elif x < 1000:
		return '500-1000ms'
	elif x < 2000:
		return '1000-2000ms'
	else :
		return '2000ms以上'

#统计不同维度的视频播放等待时长
def Video_play_Delay(xdr_path,result_path):
	if os.path.isdir(result_path + '\\视频播放等待时长'):
		pass
	else:
		os.mkdir(result_path + '\\视频播放等待时长')
	A_name = open(xdr_path + '\\final_2(包含业务类型).csv', encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['成功次数'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
	A_file = A_file[A_file['成功次数'] == 1]

	B_file = A_file.loc[:, ['网页流媒体播放等待时长(ms)']]
	B_file['播放次数'] = 1
	B_file['等待时长区间'] = B_file['网页流媒体播放等待时长(ms)'].map(segmentation_play_Delay)
	B_file = B_file.groupby(['等待时长区间'], as_index=False).agg({'播放次数': 'sum'})
	B_file['总播放次数'] = sum(B_file['播放次数'])
	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
	B_file = B_file.sort_values(by=['等待时长区间'], ascending=True)
	B_file = B_file.round({'播放次数占比(%)': 2})
	B_file.drop(['总播放次数'], axis=1, inplace=True)
	B_file.to_csv(result_path + '\\视频播放等待时长' + '\\视频播放等待时长分布.csv', sep=',', index=False)

	C_file = A_file.loc[:,['业务名称','网页流媒体播放等待时长(ms)']]
	C_file = C_file[C_file['业务名称'] != '未知业务']
	C_file = C_file.groupby(['业务名称'], as_index=False).agg({'网页流媒体播放等待时长(ms)': 'mean'})
	C_file = C_file.sort_values(by=['网页流媒体播放等待时长(ms)'], ascending=True)
	C_file = C_file.round({'网页流媒体播放等待时长(ms)': 2})
	C_file.to_csv(result_path + '\\视频播放等待时长' + '\\不同业务的视频播放等待时长.csv', sep=',', index=False)

	D_file = A_file.loc[:,['业务名称','访问IP','网页流媒体播放等待时长(ms)']]
	D_file = D_file.groupby(['业务名称','访问IP'], as_index=False).agg({'网页流媒体播放等待时长(ms)': 'mean'})
	D_file = D_file[(D_file['网页流媒体播放等待时长(ms)'] > 2000)& (D_file['业务名称'] != '未知业务')]
	D_file = D_file.sort_values(by=['网页流媒体播放等待时长(ms)'], ascending=True)
	D_file = D_file.round({'网页流媒体播放等待时长(ms)': 2})
	D_file.to_csv(result_path + '\\视频播放等待时长' + '\\视频播放等待时长较长的服务器IP地址列表.csv', sep=',', index=False)

	E_file = A_file.loc[:, ['地市', '场景', '基站名称', '基站ID', '小区名称', '小区ID', '小区经度/RRU经度', '小区纬度/RRU纬度', '网页流媒体播放等待时长(ms)']]
	E_file = E_file.groupby(['地市', '场景', '基站名称', '基站ID', '小区名称', '小区ID', '小区经度/RRU经度', '小区纬度/RRU纬度'],
	                        as_index=False).agg({'网页流媒体播放等待时长(ms)': 'mean'})
	E_file = E_file[E_file['网页流媒体播放等待时长(ms)'] > 2000]
	E_file = E_file.sort_values(by=['网页流媒体播放等待时长(ms)'], ascending=True)
	E_file = E_file.round({'网页流媒体播放等待时长(ms)': 2})
	E_file.to_csv(result_path + '\\视频播放等待时长' + '\\视频播放等待时长较长的问题小区列表.csv', sep=',', index=False)

	A_name.close()


#Zero_break_ratio中用来将时分秒形式时间转换为分钟数
def transform_TimeToMinutes(x):
	return (x.seconds+x.microseconds/1000000)/60

#Zero_break_ratio中用来判断是否为零卡顿
def break_or_not(x):
	if x==0:
		return 1
	else:
		return 0

#统计不同维度的零卡顿播放比例
def Zero_break_ratio(xdr_path,result_path):
	if os.path.isdir(result_path + '\\零卡顿播放比例'):
		pass
	else:
		os.mkdir(result_path + '\\零卡顿播放比例')
	A_name = open(xdr_path + '\\final_2(包含业务类型).csv', encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['成功次数'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
	A_file = A_file[A_file['成功次数'] == 1]

	B_file = A_file.loc[:, ['开始时间', '结束时间', '流媒体卡顿次数']]
	B_file['开始时间'] = pd.to_datetime(B_file['开始时间'])
	B_file['结束时间'] = pd.to_datetime(B_file['结束时间'])
	B_file = B_file[B_file['结束时间'] > B_file['开始时间']]
	B_file['视频时长'] = B_file['结束时间'] - B_file['开始时间']
	B_file['视频分钟数'] = B_file['视频时长'].map(transform_TimeToMinutes)
	B_file = B_file[(B_file['视频分钟数'] > 0.5)&(B_file['视频分钟数'] < 30)]
	B_file['播放次数'] = 1
	B_file['零卡顿播放次数'] = B_file['流媒体卡顿次数'].map(break_or_not)
	B_file = B_file.set_index('开始时间')
	B_file.index = pd.to_datetime(B_file.index)
	B_file = B_file.resample('10min').sum()
	B_file['开始时刻'] = B_file.index
	B_file['开始时刻'] = B_file['开始时刻'].map(transform_HMS)
	B_file = B_file.groupby(['开始时刻'], as_index=False).agg({'零卡顿播放次数': 'sum', '播放次数': 'sum'})
	B_file['零卡顿播放比例(%)'] = B_file['零卡顿播放次数'] / B_file['播放次数'] * 100
	B_file = B_file.sort_values(by=['开始时刻'], ascending=True)
	B_file = B_file.round({'零卡顿播放比例(%)': 2})
	B_file.to_csv(result_path + '\\零卡顿播放比例' + '\\不同时刻零卡顿播放比例.csv', sep=',', index=False)
	A_name.close()


#Video_break_freq中用来对视频卡顿频次分段
def segmentation_break_freq(x):
	if x == 0 :
		return '未发生卡顿'
	elif x < 0.011 :
		return '90分钟以上'
	elif x < 0.017:
		return '60-90分钟'
	elif x < 0.033:
		return '30-60分钟'
	elif x < 0.067:
		return '15-30分钟'
	elif x < 0.1:
		return '10-15分钟'
	elif x < 0.2:
		return '5-10分钟'
	elif x < 1:
		return '1-5分钟'
	elif x < 2:
		return '30秒-1分钟'
	elif x < 5.99:
		return '10-30秒'
	elif x >=5.99 :
		return '10秒以下'

#统计不同维度的视频卡顿频次
def Video_break_freq(xdr_path,result_path):
	if os.path.isdir(result_path + '\\视频卡顿频次'):
		pass
	else:
		os.mkdir(result_path + '\\视频卡顿频次')
	A_name = open(xdr_path + '\\final_2(包含业务类型).csv', encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['成功次数'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
	A_file = A_file[A_file['成功次数'] == 1]

	B_file = A_file.loc[:, ['开始时间', '结束时间', '流媒体卡顿次数']]
	B_file['开始时间'] = pd.to_datetime(B_file['开始时间'])
	B_file['结束时间'] = pd.to_datetime(B_file['结束时间'])
	B_file = B_file[B_file['结束时间'] > B_file['开始时间']]
	B_file['视频时长'] = B_file['结束时间'] - B_file['开始时间']
	B_file['视频分钟数'] = B_file['视频时长'].map(transform_TimeToMinutes)
	B_file['播放次数'] = 1
	B_file['播放卡顿频次(次/分钟)'] = B_file['流媒体卡顿次数']/B_file['视频分钟数']
	B_file['卡顿时间间隔'] = B_file['播放卡顿频次(次/分钟)'].map(segmentation_break_freq)
	B_file = B_file.groupby(['卡顿时间间隔'], as_index=False).agg({'播放次数': 'sum'})
	B_file['总播放次数'] = sum(B_file['播放次数'])
	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
	B_file = B_file.round({'播放次数占比(%)': 2})
	B_file.drop(['总播放次数'], axis=1, inplace=True)
	B_file = B_file.sort_values(by=['卡顿时间间隔'], ascending=True)
	B_file.to_csv(result_path + '\\视频卡顿频次' + '\\视频卡顿间隔区间分布.csv', sep=',', index=False)
	A_name.close()


#Break_time_ratio中用来对视频卡顿时长比例分段
def segmentation_break_time_ratio(x):
	if x == 0 :
		return '未发生卡顿'
	elif x < 1 :
		return '1%以下'
	elif x < 5:
		return '1%-5%'
	elif x < 10:
		return '5%-10%'
	elif x < 20:
		return '10%-20%'
	elif x < 30:
		return '20%-30%'
	elif x < 50:
		return '30%-50%'
	elif x < 70:
		return '50%-70%'
	elif x < 90:
		return '70%-90%'
	else:
		return '90%-100%'

#统计不同维度的卡顿时长比例
def Break_time_ratio(xdr_path,result_path):
	if os.path.isdir(result_path + '\\视频卡顿时长比例'):
		pass
	else:
		os.mkdir(result_path + '\\视频卡顿时长比例')
	A_name = open(xdr_path + '\\final_2(包含业务类型).csv', encoding='gbk')
	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
	A_file['成功次数'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
	A_file = A_file[A_file['成功次数'] == 1]

	B_file = A_file.loc[:, ['开始时间', '结束时间','流媒体卡顿时长(ms)']]
	B_file['开始时间'] = pd.to_datetime(B_file['开始时间'])
	B_file['结束时间'] = pd.to_datetime(B_file['结束时间'])
	B_file = B_file[B_file['结束时间'] > B_file['开始时间']]
	B_file['视频时长'] = B_file['结束时间'] - B_file['开始时间']
	B_file['视频分钟数'] = B_file['视频时长'].map(transform_TimeToMinutes)
	B_file['播放次数'] = 1
	B_file['卡顿时长比例(%)'] = B_file['流媒体卡顿时长(ms)'] / (B_file['视频分钟数']*60*1000)*100
	B_file['卡顿时长比例区间'] = B_file['卡顿时长比例(%)'].map(segmentation_break_time_ratio)
	B_file = B_file.groupby(['卡顿时长比例区间'], as_index=False).agg({'播放次数': 'sum'})
	B_file['总播放次数'] = sum(B_file['播放次数'])
	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
	B_file = B_file.round({'播放次数占比(%)': 2})
	B_file.drop(['总播放次数'], axis=1, inplace=True)
	B_file = B_file.sort_values(by=['卡顿时长比例区间'], ascending=True)
	B_file.to_csv(result_path + '\\视频卡顿时长比例' + '\\视频卡顿时长比例区间分布.csv', sep=',', index=False)
	A_name.close()





#Video_download_Speed中用来对视频码率分段
# def segmentation_speed(x):
# 	if x < 0.5:
# 		return '0-0.5Mbps'
# 	elif x < 1:
# 		return '0.5-1Mbps'
# 	elif x < 1.5:
# 		return '1-1.5Mbps'
# 	elif x < 2:
# 		return '1.5-2Mbps'
# 	elif x < 2.5:
# 		return '2-2.5Mbps'
# 	elif x < 3:
# 		return '2.5-3Mbps'
# 	elif x < 4:
# 		return '3-4Mbps'
# 	elif x < 5:
# 		return '4-5Mbps'
# 	elif x < 10:
# 		return '5-10Mbps'
# 	else :
# 		return '10Mbps以上'
#
# def Video_download_Speed(xdr_path,result_path):
# 	if os.path.isdir(result_path + '\\视频下载速率'):
# 		pass
# 	else:
# 		os.mkdir(result_path + '\\视频下载速率')
# 	A_name = open(xdr_path + '\\final_2(包含业务类型).csv', encoding='gbk')
# 	A_file = pd.read_csv(A_name, sep=',', encoding='utf-8')
# 	A_file['是否播放成功'] = pd.Series(map(lambda x, y: success_or_not(x, y), A_file['实际下载的视频流媒体字节数(字节)'], A_file['流媒体码率(kbps)']))
# 	A_file = A_file[A_file['是否播放成功'] == 1]
# 	A_file['视频下载速率(Mbps)'] = A_file['实际下载的视频流媒体字节数(字节)']*8/(1000*1000)/(A_file['网页流媒体下载时长(ms)']/1000)
# 	A_file['速率区间'] = A_file['视频下载速率(Mbps)'].map(segmentation_speed)
# 	A_file['播放次数'] = 1
#
# 	B_file = A_file.groupby(['速率区间'], as_index=False).agg({'总流量(MB)': 'sum', '播放次数': 'sum'})
# 	B_file['全流量(MB)'] = sum(B_file['总流量(MB)'])
# 	B_file['流量占比(%)'] = B_file['总流量(MB)'] / B_file['全流量(MB)'] * 100
# 	B_file['总播放次数'] = sum(B_file['播放次数'])
# 	B_file['播放次数占比(%)'] = B_file['播放次数'] / B_file['总播放次数'] * 100
# 	B_file = B_file.sort_values(by=['速率区间'], ascending=True)
# 	B_file.drop(['全流量(MB)', '总播放次数'], axis=1, inplace=True)
# 	B_file = B_file.round(2)
# 	B_file.to_csv(result_path + '\\视频下载速率' + '\\不同速率视频播放次数及流量占比.csv', sep=',', index=False)
#
# 	C_file = A_file.loc[:, ['业务名称', '视频下载速率(Mbps)']]
# 	C_file = C_file.groupby(['业务名称'], as_index=False).agg({'视频下载速率(Mbps)': 'mean'})
# 	C_file = C_file[C_file['业务名称'] != '未知业务']
# 	C_file = C_file.sort_values(by=['视频下载速率(Mbps)'], ascending=True)
# 	C_file = C_file.round({'视频下载速率(Mbps)': 2})
# 	C_file.to_csv(result_path + '\\视频下载速率' + '\\不同业务视频下载速率.csv', sep=',', index=False)
#
# 	D_file = A_file.loc[:, ['业务名称', '视频下载速率(Mbps)','流媒体码率(kbps)']]
# 	D_file = D_file[D_file['流媒体码率(kbps)'] != 0]
# 	D_file['流媒体码率(Mbps)'] = D_file['流媒体码率(kbps)']/1000
# 	D_file = D_file.groupby(['业务名称'], as_index=False).agg({'视频下载速率(Mbps)': 'mean','流媒体码率(Mbps)': 'mean'})
# 	D_file = D_file[D_file['业务名称'] != '未知业务']
# 	D_file['速率码率比'] = D_file['视频下载速率(Mbps)']/D_file['流媒体码率(Mbps)']
# 	D_file = D_file.sort_values(by=['速率码率比'], ascending=True)
# 	D_file.drop(['视频下载速率(Mbps)', '流媒体码率(Mbps)'], axis=1, inplace=True)
# 	D_file = D_file.round({'速率码率比': 2})
# 	D_file.to_csv(result_path + '\\视频下载速率' + '\\不同业务视频速率码率比.csv', sep=',', index=False)










