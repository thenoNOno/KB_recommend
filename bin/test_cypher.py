#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *


class test_data(object):
    """
    测试数据获取常用工具集

    """
    def save_test_data(self,tick):
        """
        执行测试cypher并将查询结果存储到csv中

        """
        cypher=f'''
match p=shortestPath((n:outside:company)-[*0..2]-(n_e:outside:company))
where n.listed='1' and n_e.listed='1' and n.tick='{tick}'
with *,nodes(p)[0] as n_1,nodes(p)[1] as n_2,nodes(p)[2] as n_3,nodes(p)[3] as n_4,nodes(p)[4] as n_5,relationships(p)[0] as r_1,relationships(p)[1] as r_2
match p_n=(n_e)-[w:watching]-(n_n:outside:news)
where n_n.datetime>='2018-05-18' and n_n.datetime<'2018-05-19'
return distinct
length(p) as len,n_1.name as step1,toFloat(coalesce(r_1.income,0.0)) as income_1,n_2.name as step2,toFloat(coalesce(r_2.income,0.0)) as income_2,n_3.name as step3,n_n.datetime as datetime,n_n.title as title,n_n.polarity as polarity,n_1.name as company,n_1.tick as tick,n_e.name as news_company,n_e.tick as news_company_tick,n_e.closeness as closeness,n_n.pagerank as pagerank
order by len asc,income_1 desc,income_2 desc,closeness desc,pagerank desc
limit 20
        '''
        c=writer()
        print(cypher)
        filename='./test/old_'+tick+'.csv'
        data=c.save_data_csv(cypher,filename)
        return cypher


def main():
    """
    程序执行入口

    """

    test=test_data()
    c=carrier()
    # tick_list=['300094']
    #tick_list=['002447','601360','300104','002230','002557','000063']
    tick_list=['002447','600039','000860','000596','300135','601339','002680','300601','300296','002618','601360','300104','002230','002557','000063','601939','601988']
    for tick in tick_list:
        data=test.save_test_data(tick)
    print('已将结果保存为csv')

if __name__ == '''__main__''':
    main()
