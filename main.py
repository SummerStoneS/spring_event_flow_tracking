import pandas as pd
import os
import re
import jieba
import glob
import warnings
from nlp_tools import get_word_frequency
import itertools
from data_tools import get_melted_data_for_one_year
warnings.filterwarnings("ignore")

"""
by year by date 看Spring不同event的热词的trend, 分析产品上新的flow

"""

# 输出文件
output_datasource_file = 'data_source.h5'
source_key_2023_raw = '2023'
source_key_2022_raw = '2022'
product_tag_2023 = 'product_event_keywords_tag_2023'
product_tag_2022 = 'product_event_keywords_tag_2022'


def get_event_dict(cny_words):
	base_event_dict = {'春夏新风尚': ['春夏新品', '夏季新款', '显瘦', '冰激凌', '马卡龙', '樱花季', '露脐', '收腹'],
	              'cny': ['新年', '兔年', '红', '新春', '本命', 'CNY', '年货', '大吉大利', '日进斗金', '春晚', 
	                      '财源滚滚', '发财','春节', '虎'], 
	              '38': ['女王','女神','3\.8','妇女','[^\d#]38[^\d]','三八'],
	              '情人节': ['情侣', '男女同款', '情人', '女友', '老婆', '礼物', '送礼','爱心','浪漫'],
	              '元旦': ['一元复始','新年','本命年','双旦','虎年','元旦'],
	              '国潮': ['国潮', '国风', '中国红'],
	              'Gifting': ['圣诞', '新年礼物', '冬日', '缤纷','节日'],
	              '早春': ['早春', '踏青', '野餐', '春游','春季','春日','春装'],
	              'Newness': ['新品', '新款', '上新']
	             }
	
	event_dict23 = base_event_dict.copy()
	event_dict22 = base_event_dict.copy()

	cny_2023_words = cny_words.query("Year == 2023")['Key Words'].tolist()
	cny_2022_words = cny_words.query("Year == 2022")['Key Words'].tolist()

	event_dict23['cny'] = list(set(base_event_dict['cny'] + cny_2023_words))
	event_dict22['cny'] = list(set(base_event_dict['cny'] + cny_2022_words))

	event_dict = {2023: event_dict23, 2022: event_dict22}

	return event_dict



if __name__ == '__main__':

	
	# 不同event的关键词定义
	add_cny_words = pd.read_excel("QBT数据CNY标签需求.xlsx")   # 把excel里新年关键词加到上面的event关键词词典里2022,2023各有一个版本
	event_dict = get_event_dict(add_cny_words)

	# 情报通数据处理
	for year in [2022, 2023]:
	    data_folder = f'CNY Flow data/{year}'        # 原始输入文件
	    one_year_data = get_melted_data_for_one_year(data_folder)
	    one_year_data.to_hdf(output_datasource_file, key=f'{year}')

	# 分别输出2022,2023两年SP各个event的关键词的trend
	for year in [2022, 2023]:
	    datapreprocessor = DataPreprocessor(year)
	    datapreprocessor.convert_object_to_numerical(['参考价格', '销量', '销售额'], data_type='float')
	    datapreprocessor.cal_msrp_amt()
	    datapreprocessor.load_product_tag()          

	    result_file = pd.ExcelWriter(f"result/sp{year}_event_keywords_daily_sales.xlsx")

	    for event, _ in event_dict[year].items():
	        print(event)
	        one_event_data = datapreprocessor.get_one_event_data(event)    # 取出event标签=1的所有记录
	        event_tot_daily = one_event_data.groupby(["date"]).agg({'销量':sum, '销售额':sum, 'msrp_amt':sum}) # event的total daily demand
	        event_keywords_melted = one_event_data.groupby(['date','销量', '销售额','msrp_amt']).apply(lambda x: expand_joint_keywords_to_rows(x, col=f'{event}_keyword')).reset_index()
	        # 如果是cny，因为关键词太多了，取销量最高的前40个
	        if event == 'cny':
	            top_heat_words = event_keywords_melted.groupby('cny_keyword')['销量'].sum().sort_values(ascending=False).index[:40]  # 去销量最高的前40个词
	            event_keywords_melted = event_keywords_melted[event_keywords_melted['cny_keyword'].isin(top_heat_words)]

	        keywords_daily = event_keywords_melted.pivot_table(index=['date'], columns=[f'{event}_keyword'], values='销量', aggfunc='sum')  #event 里每一个keyword的daily demand,每个keyword是一列
	        keywords_daily = keywords_daily.add_suffix("_销量")
	        event_all = pd.merge(event_tot_daily, keywords_daily, on='date',how='outer').reset_index()
	        event_all.to_excel(result_file, sheet_name=event, index=False)

	    result_file.close()
	
	## CNY pack by brand analysis
	#### 每个品牌把包含CNY关键词的产品打上CNY的tag，然后看下cny pack 的weekly sales qty/revenue/md的变化

	for year in [2023, 2022]:
	    datapreprocessor = DataPreprocessor(year)
	    datapreprocessor.convert_object_to_numerical(['参考价格', '销量', '销售额'], data_type='float')
	    datapreprocessor.cal_msrp_amt()
	    cny_data = datapreprocessor.get_one_event_data('cny')
	    cny_result_file = pd.ExcelWriter(f"result/cny_{year}.xlsx")
	    # 1.raw data
	    cny_data.to_excel(cny_result_file, sheet_name='raw', index=False)
	    # 2.cny pack total sales by brand
	    cny_data.groupby("掌柜名称")['销量'].sum().sort_values(ascending=False).to_excel(cny_result_file, sheet_name='brand_tot') 
	    # 3.cny pack weekly sales performance by brand
	    cny_data['year_week'] = cny_data['date'].apply(lambda x: f"{x.year}-{x.week}")
	    cny_by_brand = cny_data.groupby(['掌柜名称','year_week']).agg({'销量': sum, '销售额': sum, 'msrp_amt': sum, 'date':'first'})
	    cny_by_brand['md'] = cny_by_brand['销售额'] / cny_by_brand['msrp_amt']
	    cny_by_brand.reset_index().to_excel(cny_result_file, sheet_name='brand_weekly', index=False)
	    cny_result_file.close()


	# # 关键词筛选，每个月份输出一份词汇的表
	# # 标题分词词频统计
	# save_file = pd.ExcelWriter("result/cny2023分词bymonth.xlsx")
	# for month in [12,1,2]:
	#     monthly_data = cny2023.query(f"month == {month}")
	#     name_list = monthly_data['宝贝名称'].drop_duplicates().tolist()
	#     word_freq_df = get_word_frequency(name_list)
	#     word_freq_df.to_excel(save_file,sheet_name = f'cny23_{month}')
	# save_file.close()