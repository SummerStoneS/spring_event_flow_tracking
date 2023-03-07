import pandas as pd
import jieba
from collections import Counter
import re
import warnings
warnings.filterwarnings("ignore")



special_words = ['双十一', '双11']
for word in special_words:
	jieba.add_word(word)


def tokenize(initial_title):
	clean_title = re.sub("[^\u4e00-\u9fa5^a-z^A-Z^0-9，。！,\.! |]", "", initial_title)
	words = list(jieba.cut(clean_title))
	words = [x for x in words if x.strip() != ""]
	return words

def get_word_frequency(name_list):
	all_words = []      # 所有标题的分词
	for title in name_list:
	    word_list = tokenize(title)
	    all_words.extend(word_list)
	counter = Counter(all_words)
	word_freq_list = counter.most_common(100000)
	return pd.DataFrame(word_freq_list, columns=['word', 'freq'])