#!/usr/bin/python3
# -*- coding:utf-8 -*-

# @Time      :  2018/4/26 10:49
# @Auther    :  WangYang
# @Email     :  evilwangyang@126.com
# @Project   :  XDR
# @File      :  organize_file.py
# @Software  :  PyCharm Community Edition

# ********************************************************* 
import os
import shutil
import gzip
import tarfile
import glob
import pandas as pd


#XDR数据Streaming业务统计单据表头
video_header = ['特定协议话单类型','开始时间','结束时间','单据状态','IMSI','IMEI','手机号码','SGSN或eNodeB用户面IP',
               'SGSN或eNodeB用户面TEID','GGSN或SGW用户面IP','GGSN或SGW用户面TEID','位置区编码','路由区','CI号码','网络类型',
               'APN','上行持续时长(ms)','下行持续时长(ms)','上行IP包数','下行IP包数','上行流量(字节)','下行流量(字节)','终端IP',
                '终端端口','访问IP','服务器端口','应用大类','应用子类','应用类别扩展字段','应用类别扩展字段','应用类别扩展字段',
                '4层协议类型','应用层协议','上层TCP层乱序个数','下行TCP层乱序个数','上行TCP乱序流量','下行TCP乱序流量',
                '上行IP层重传个数','下行IP层重传个数','上行重传流量','下行重传流量','上行SYN到下行SYN ACK(ms)',
                '下行syn ack到上行ack(ms)','上行IP分段包个数','下行IP分段包个数','上行IP分段流量','下行IP分段流量',
                'TCP建链成功到第一条事务请求的时延(ms)','TCP建链成功到第一条事务响应的时延(ms)','TCP建链时UE侧的接收窗口',
                '三次握手协商最小MSS','TCP建链重试次数','TCP链接标识','上行RTT次数','上行RTT总时延(ms)','下行RTT次数',
                '下行RTT总时延(ms)','UE侧报告零窗口的次数','UE侧零窗口持续的时间总和(ms)','SP侧报告零窗口的次数',
                'SP侧零窗口持续的时间总和(ms)','上行TCP丢包数','下行TCP丢包数','上行TCP丢失流量','下行TCP丢失流量',
                '网页流媒体播放等待时长(ms)','网页流媒体下载时长(ms)','流媒体码率(kbps)','实际下载的视频流媒体字节数(字节)',
                '流媒体标识','流媒体卡顿次数','流媒体卡顿时长(ms)','视频播放失败原因次数统计']

#字段“单据状态”取值说明
CDRSTAT = {0:'正常',1:'异常',2:'掉线',3:'截断',4:'超时'}

#字段“4层协议类型”取值说明
L4_PROTOCAL = {0:'TCP',1:'UDP'}

#字段“TCP链接标识”取值说明
TCP_LINK_STATUS = {0:'成功',1:'失败'}

#字段“流媒体标识”取值说明
STREAMIND = {0:'网页内容非流媒体',1:'网页内容是流媒体'}


#从XDR全量数据文件(包含多类表单)中筛选出Streaming业务表单文件，拷贝到指定文件夹
def choose_file(src_path,dst_path,str_name):#src_path为源文件夹，dst_path为目标文件夹，str_name为目标文件名包含的字段
	if os.path.isdir(dst_path + '\\' + '视频业务原始数据'):
		pass
	else:
		os.mkdir(dst_path + '\\' + '视频业务原始数据')
	for file in os.listdir(src_path):
		if str_name in os.path.split(file)[1]:
			shutil.copyfile(src_path+'\\'+file,dst_path+'\\视频业务原始数据\\'+file)


#解压Streaming业务表单压缩包(两层压缩)，生成txt文件
def decompress_file(path):
	for file in os.listdir(path+'\\视频业务原始数据\\'):
		f_name = file.replace(".gz","")
		g_file = gzip.GzipFile(path+'\\视频业务原始数据\\'+file)
		if os.path.isdir(path+'\\原始数据第一层解压'):
			pass
		else:
			os.mkdir(path+'\\原始数据第一层解压')
		open(path+'\\原始数据第一层解压\\'+f_name,'wb+').write(g_file.read())
		g_file.close()

	for file in os.listdir(path+'\\原始数据第一层解压\\'):
		tar = tarfile.open(path+'\\原始数据第一层解压\\'+file)
		names = tar.getnames()
		if os.path.isdir(path+'\\'+'原始数据第二层解压'):
			pass
		else:
			os.mkdir(path+'\\'+'原始数据第二层解压')
		for name in names:
			tar.extract(name,path+'\\原始数据第二层解压\\')
		tar.close()


#将所有Streaming业务表单的txt文件合并成一个全量的txt文件
def combine_file(path):
	file_path = path + '\\原始数据第二层解压'
	os.chdir(file_path)
	file_list = glob.glob('*.txt')
	chunkSize = 100000
	chunks = []
	for file in file_list:
		f_name = open(file_path + '\\' + file)
		sub_file = pd.read_table(f_name, header=None, sep='|', encoding='utf-8', iterator=True)
		loop = True
		while loop:
			try:
				chunk = sub_file.get_chunk(chunkSize)
				chunks.append(chunk)
			except StopIteration:
				loop = False
				#print("Iteration is stopped.")
		f_name.close()
	result = pd.concat(chunks, ignore_index=True)
	if os.path.isdir(path + '\\视频业务表单合并版'):
		pass
	else:
		os.mkdir(path + '\\' + '视频业务表单合并版')
	result.to_csv(path+'\\视频业务表单合并版\\视频业务表单合并版.csv',sep=',',index=False,header=video_header)


#将Streaming业务全量表单匹配上工参数据
def match_parameter(xdr_path,parameter_path):
	file_xdr = xdr_path + '\\视频业务表单合并版\\视频业务表单合并版.csv'
	file = os.listdir(parameter_path)[0]
	file_parameter = parameter_path + '\\' +file
	f_xdr = open(file_xdr)
	xdr = pd.read_csv(f_xdr,sep=',',encoding='utf-8')
	f_parameter = open(file_parameter)
	parameter = pd.read_csv(f_parameter,sep=',',encoding='utf-8')
	outfile = pd.merge(xdr,parameter,how='inner',left_on='CI号码',right_on='CI号码')
	outfile.to_csv(xdr_path+'\\视频业务表单合并版\\视频业务表单合并版1(匹配工参).csv',sep=',',index=False)
	f_xdr.close()
	f_parameter.close()


#将Streaming业务全量表单匹配上业务分类
def match_category(xdr_path,category_path):
	A_category_name = open(category_path + '\\业务大类表.csv', encoding='utf-8')
	A_category_file = pd.read_csv(A_category_name, sep=',', encoding='utf-8')
	B_category_name = open(category_path + '\\应用小类表.csv', encoding='utf-8')
	B_category_file = pd.read_csv(B_category_name, sep=',', encoding='utf-8')
	xdr_name = open(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版1(匹配工参).csv', encoding='gbk', errors='ignore')
	xdr_file = pd.read_csv(xdr_name, sep=',', encoding='utf-8')
	result_1 = pd.merge(xdr_file, A_category_file, how='left', left_on='应用大类', right_on='序号')
	result_2 = pd.merge(result_1, B_category_file, how='left', left_on=['应用大类', '应用子类'], right_on=['业务类型', '应用编号'])
	result_2['大类业务类型'].fillna('未知类型', inplace=True)
	result_2['业务名称'].fillna('未知业务', inplace=True)
	result_2.drop(['序号', '业务类型', '应用编号'], axis=1, inplace=True)
	result_2.to_csv(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版2(匹配业务分类).csv', sep=',', index=False)
	A_category_name.close()
	B_category_name.close()
	xdr_name.close()


#用于match_mobile中将TAC字段补全为8位
def fill_TAC(x):
	return str(x).zfill(8)

#用于match_mobile中从IMEI中截取TAC
def cut_TAC(x):
	return str(x)[:8]

#将Streaming业务全量表单匹配上终端信息
def match_mobile(xdr_path,mobile_path):
	xdr_name = open(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版2(匹配业务分类).csv', encoding='gbk', errors='ignore')
	xdr_file = pd.read_csv(xdr_name, sep=',', encoding='utf-8')
	xdr_file['TAC'] = xdr_file['IMEI'].map(cut_TAC)
	mobile_name = open(mobile_path + '\\终端TAC库.csv', encoding='gbk',errors='ignore')
	mobile_file = pd.read_csv(mobile_name, sep=',', encoding='utf-8')
	mobile_file['TAC'] = mobile_file['TAC'].map(fill_TAC)
	result = pd.merge(xdr_file, mobile_file, how='left', on='TAC')
	result['品牌'].fillna('未知品牌', inplace=True)
	result['型号'].fillna('未知型号', inplace=True)
	result['终端类型'].fillna('未知类型', inplace=True)
	result['操作系统'].fillna('未知系统', inplace=True)
	result.to_csv(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版3(匹配终端信息).csv', sep=',', index=False)
	xdr_name.close()
	mobile_name.close()


#将Streaming业务全量表单匹配上其他枚举值
def match_other(xdr_path,protocol_path):
	xdr_name = open(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版3(匹配终端信息).csv', encoding='gbk', errors='ignore')
	xdr_file = pd.read_csv(xdr_name, sep=',', encoding='utf-8')
	protocol_name = open(protocol_path + '\\应用层协议类型.csv', encoding='gbk',errors='ignore')
	protocol_file = pd.read_csv(protocol_name, sep=',', encoding='utf-8')
	result = pd.merge(xdr_file, protocol_file, how='left', left_on='应用层协议', right_on='协议编码')
	result['应用层协议类型'].fillna('未知协议', inplace=True)
	result.drop(['协议编码'], axis=1, inplace=True)
	result['单据状态'] = result['单据状态'].replace(CDRSTAT)
	result['4层协议类型'] = result['4层协议类型'].replace(L4_PROTOCAL)
	result['TCP链接标识'] = result['TCP链接标识'].replace(TCP_LINK_STATUS)
	result['流媒体标识'] = result['流媒体标识'].replace(STREAMIND)
	result.to_csv(xdr_path + '\\视频业务表单合并版\\视频业务表单合并版4(匹配其他枚举值).csv', sep=',', index=False)
	xdr_name.close()
	protocol_name.close()


#新增一些分析常用的字段
def calculate_publicVar(path):
	xdr_name = open(path + '\\视频业务表单合并版\\视频业务表单合并版4(匹配其他枚举值).csv', encoding='gbk', errors='ignore')
	xdr_file = pd.read_csv(xdr_name, sep=',', encoding='utf-8')
	xdr_file['总流量(字节)'] = xdr_file['上行流量(字节)']+xdr_file['下行流量(字节)']
	xdr_file['总流量(MB)'] = xdr_file['总流量(字节)']/1000000
	xdr_file['总流量(GB)'] = xdr_file['总流量(MB)']/1000
	xdr_file.to_csv(path + '\\视频业务表单合并版\\视频业务表单合并版5(新增常用分析字段).csv', sep=',', index=False)
	xdr_name.close()


