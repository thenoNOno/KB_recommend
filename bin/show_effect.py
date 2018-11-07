# -*- coding: utf-8 -*
from carrier import *
import pandas as pd
import numpy as np

class effect(object):
    """
    量化地验证训练效果

    """

    def __init__(self,test_file,recommend_file='0'):
        """
        初始化,根据需要制作结果集,计算召回率

        """
        test_data = self.get_data(test_file)
        # 从测试集获取推荐的开始节点,从图谱获取推荐结果集
        start_nodes = test_data['n'].drop_duplicates()
        print('预测次数:',start_nodes.count())
        if recommend_file == '0':
            recommend_file = test_file+'_recall.csv'
            ru = rule()
            ru.soft_remove(recommend_file)
            for start_node in start_nodes:
                self.save_recommend(start_node,recommend_file)
        elif recommend_file == '1':
            recommend_file = test_file+'_recall.csv'
        else:
            pass
        recommend_data = self.get_data(recommend_file)
        F1 = self.get_recall(recommend_data,test_data)
        print('F1-Measure:',F1)

    def save_recommend(self,start_node,filename):
        """
        从图谱中,获取新闻推荐结果集,存储到本地csv文件

        """
        start_node = start_node
        datetime = "n_n.datetime>='2018-05-10' and n_n.datetime<'2018-05-19'"
        cypher = f'''
match p=shortestPath((n:outside)-[*0..2]-(n_e:outside:company))
where n.listed='1' and n_e.listed='1' and n.pid='{start_node}'
with *,nodes(p)[0] as n_1,nodes(p)[1] as n_2,nodes(p)[2] as n_3,nodes(p)[3] as n_4,nodes(p)[4] as n_5,relationships(p)[0] as r_1,relationships(p)[1] as r_2
match p_n=(n_e)-[w:watching]-(n_n:outside:news)
where {datetime}
return distinct
length(p) as len,n_1.pid as n,n_n.pid as n_e,n_n.datetime as datetime,n_n.pagerank as pagerank,100.0-toFloat(coalesce(r_1.norm,100.0)) as distance_1,100.0-toFloat(coalesce(r_2.norm,100.0)) as distance_2,100.0-toFloat(coalesce(w.norm,100.0)) as distance_3
order by distance_1^2+distance_2^2+distance_3^2 asc,len asc,datetime desc,pagerank desc
limit 8
        '''
        # 如果文件已存在,不追加写入列名
        if os.path.exists(filename):
            header = False
        else:
            header = True
        w = writer()
        w.save_data_csv(cypher,filename,header=header)
        return filename

    def get_data(self,filename):
        """
        从本地文件,获取验证集/结果集,返回data

        """
        filename = filename
        dataframe = pd.read_csv(filename,sep='\t|,')
        data = dataframe[['n','n_e']]
        return data

    def get_recall(self,recommend_data,test_data):
        """
        根据结果集与测试集,计算并返回召回率,精准率

        按推荐点分组计算召回率/精准率,求平均值
        """
        tmp_TP = pd.merge(recommend_data,test_data
                      ,how='inner'
                      ,on=['n','n_e']).groupby(['n']).count()
        tmp_TPFN = test_data.groupby(['n']).count()
        tmp_TPFP = recommend_data.groupby(['n']).count()
        tmp_recall = pd.merge(tmp_TP,tmp_TPFN
                       ,how='inner'
                       ,on=['n'])
        recall = tmp_recall.eval('n_e=n_e_x/n_e_y'
                                 ,inplace=False)['n_e'].mean()
        tmp_precision = pd.merge(tmp_TP,tmp_TPFP
                       ,how='inner'
                       ,on=['n'])
        precision = tmp_precision.eval('n_e=n_e_x/n_e_y'
                                       ,inplace=False)['n_e'].mean()
        #recall=TP/TPFN
        #precision=TP/TPFP
        #F1=(2*recall*precision)/(recall+precision)
        print('召回率:',recall)
        print('准确率:',precision)
        F1 = np.divide(2*recall*precision,recall+precision)
        return F1


def main():
    """
    程序执行入口

    """
    print('结果集/训练集--------')
    #l = effect('/projects/KB_recommend/test/1_train_0919.txt','0')
    l = effect('/projects/KB_recommend/test/1_train_0919.txt','1')
    print('结果集/测试集--------')
    #l = effect('/projects/KB_recommend/test/2_train_0919.txt','0')
    l = effect('/projects/KB_recommend/test/2_train_0919.txt','1')

if __name__=='''__main__''':
    main()
