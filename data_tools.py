import pandas as pd
import re
import warnings

warnings.filterwarnings("ignore")


index_cols = ['掌柜名称','宝贝名称','一级类目','二级类目','三级类目','四级类目','链接', '品牌','参考价格','上架时间','销售额总量','销量总量','成交均价']


def expand_joint_keywords_to_rows(df, col='keywords'):
    return pd.DataFrame({col: itertools.chain(*df[col].str.split(','))})


def get_event_keywords(sentence, event, keyword_match_list):
    # （event的tag，每条产品名称包含的该event下的所有keyword)
    keywords = []
    for pattern in keyword_match_list:
        try:
            if re.search(f'{pattern}', sentence) != None:
                if pattern == '3\.8':
                    pattern = '3.8'
                if pattern == '[^\d#]38[^\d]':
                    pattern ='38'
                keywords.append(pattern)
        except Exception as E:
            print(pattern, sentence)
            raise ValueError
    if len(keywords) > 0:
        return (1, ','.join(keywords))
    else:
        return (0, None) 
    
    
class DataPreprocessor:
    def __init__(self, year):
        self.year = year
        self.load_data()
        self.init_data()
        self.product_tag = None
        
    def load_data(self):
        self.data = pd.read_hdf(output_datasource_file, key=str(self.year))
    
    def init_data(self):
        self.data = self.data[self.data["宝贝名称"].notnull()]
        self.data['宝贝名称'] = self.data['宝贝名称'].str.upper()
        
    def extract_date_info(self, date_col):
        self.data = pd.concat([self.data, self.data[date_col].dt.isocalendar()], axis=1)
        self.data['month'] = self.data[date_col].dt.month

    def convert_object_to_numerical(self, cols, data_type='int'):
        for col in cols:
            self.data[col] = self.data[col].astype(str).str.replace(",",'')
            self.data[col] = self.data[col].astype(data_type)
    
    def cal_msrp_amt(self):
        self.data['msrp_amt'] = self.data['参考价格'] * self.data['销量']        
            
    def tag_event(self):
        """
            每个event生成新的两列，一列是event的tag（0,1），另一列是event下出现的关键词
        """
        self._event_dict = event_dict[self.year]
        self.product_tag = pd.DataFrame(self.data['宝贝名称'].drop_duplicates())
        for event, keywords in self._event_dict.items():
            print(event)
            self.product_tag[event] = self.product_tag['宝贝名称'].apply(lambda x: get_event_keywords(x, event, keywords))
            self.product_tag[f"{event}_keyword"] = self.product_tag[event].apply(lambda x: x[1])
            self.product_tag[event] = self.product_tag[event].apply(lambda x: x[0])
        
        self.product_tag.to_hdf(output_datasource_file, key=f'product_event_keywords_tag_{self.year}')
            
    def load_product_tag(self):
        try:
            self.product_tag = pd.read_hdf(output_datasource_file, key=f'product_event_keywords_tag_{self.year}')
        except:
            self.tag_event()

    def get_one_event_data(self, event):
        "选择一个event标签，提取所有带有这个event标签的产品的sales数据"
        if self.product_tag is None:
            self.load_product_tag()
        one_event_data = self.product_tag[self.product_tag[event] == 1]
        one_event_data = pd.merge(one_event_data[['宝贝名称', event, f"{event}_keyword"]], self.data, on='宝贝名称', how='left')
        return one_event_data


def melt_one_metric(data, measure_type='销量'):
    """
    "转换情报通的数据---每天是一个列，销量和销售额各一列"
    melt一个维度，销量或者销售额
    
    """
    data_columns = data.columns
    metric_cols = [x for x in data_columns if measure_type in x and '总量' not in x]
    renamed_metric_cols = [re.sub(measure_type,'',x) for x in metric_cols]
    metric_data = data[index_cols + metric_cols]   # 只有销量或者销售额的数据
    metric_data = metric_data.rename(columns=dict(zip(metric_cols, renamed_metric_cols)))
    metric_data = metric_data.melt(id_vars = index_cols, value_vars=renamed_metric_cols, var_name='date', value_name=measure_type)
    metric_data['date'] = pd.to_datetime(metric_data['date'])
    return metric_data


def process_one_month_data(data):
    """
    合并melt后的一个月的销量和销售额的数据
    """
    qty_data = melt_one_metric(data, measure_type='销量')
    rev_data = melt_one_metric(data, measure_type='销售额')
    new_index_columns = index_cols + ['date']
    one_month_melt_data = pd.merge(qty_data, rev_data, how='outer', on=new_index_columns)
    return one_month_melt_data


def get_melted_data_for_one_year(data_folder):
    data_path_list = glob.glob(os.path.join(data_folder, '*.csv'))
    one_cny_data = pd.DataFrame()
    for data_file in data_path_list:
        data = pd.read_csv(data_file)
        one_month_melt_data = process_one_month_data(data)
        one_cny_data = pd.concat([one_cny_data, one_month_melt_data])   # 合并同一年CNY的数据
    one_cny_data['cny_year'] = year
    return one_cny_data


