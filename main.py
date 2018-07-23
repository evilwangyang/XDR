#!/usr/bin/python3
# -*- coding:utf-8 -*-

# @Time      :  2018/5/7 11:07
# @Auther    :  WangYang
# @Email     :  evilwangyang@126.com
# @Project   :  XDR
# @File      :  main.py
# @Software  :  PyCharm Community Edition

# ********************************************************* 
import organize_file
import analysis

src_dir = 'E:\\Work\\2018\\XDR数据研究\\XDR原始数据'
dst_dir = 'E:\\Work\\2018\\XDR数据研究\\视频业务数据'
parameter_dir = 'E:\\Work\\2018\\XDR数据研究\\分析所需其他数据\\工参'
category_dir = 'E:\\Work\\2018\\XDR数据研究\\分析所需其他数据\\业务分类'
mobile_dir = 'E:\\Work\\2018\\XDR数据研究\\分析所需其他数据\\终端信息'
protocol_dir = 'E:\\Work\\2018\\XDR数据研究\\分析所需其他数据\\应用层协议类型'
result_dir = 'E:\\Work\\2018\\XDR数据研究\\分析结果'
sub_str = 'STREAMING'

if __name__ == '__main__':
	# organize_file.choose_file(src_dir,dst_dir,sub_str)
	# organize_file.decompress_file(dst_dir)
	# organize_file.combine_file(dst_dir)
	# organize_file.match_parameter(dst_dir,parameter_dir)
	# organize_file.match_category(dst_dir,category_dir)
	# organize_file.match_mobile(dst_dir,mobile_dir)
	# organize_file.match_other(dst_dir,protocol_dir)
	# organize_file.calculate_publicVar(dst_dir)

	# analysis.OTT_ratio(dst_dir,result_dir)
	# analysis.Video_user(dst_dir,result_dir)
	# analysis.data_period(dst_dir,result_dir)
	# analysis.data_time_duration(dst_dir,result_dir)
	# analysis.time_codeRate(dst_dir,result_dir)
	analysis.Video_play_SuccessRate(dst_dir,result_dir)
	# analysis.Video_play_Delay(dst_dir+'\\final file',result_dir)
	# analysis.Zero_break_ratio(dst_dir+'\\final file',result_dir)
	# analysis.Video_break_freq(dst_dir+'\\final file',result_dir)
	# analysis.Break_time_ratio(dst_dir+'\\final file',result_dir)
	# analysis.Video_download_Speed(dst_dir+'\\final file',result_dir)
