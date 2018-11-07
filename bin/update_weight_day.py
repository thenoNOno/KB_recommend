#!/usr/bin/env python
# -*- coding: utf-8 -*
from datetime import datetime
from carrier import *
import numpy as np


class compute_closeness(object):
    """
    更新closeness

    """

    def reset_label(self):
        """
        重置待计算节点

        """
        cypher_0 = "match (n:outside) remove n:weight_cln;"
        cypher_1 = "match (n:outside:company) set n:weight_cln;"
        cypher_2 = "match (n:outside:product) set n:weight_cln;"
        cypher_3 = "match (n:outside:industry) set n:weight_cln;"
        c = carrier()
        c.run_cypher(cypher_0)
        c.run_cypher(cypher_1)
        c.run_cypher(cypher_2)
        c.run_cypher(cypher_3)
        print("已重置待计算标签")
        return cypher_0

    def compute(self):
        """
        计算closeness

        """
        cypher_0 = "match (n:outside) remove n:weight_cln;"
        cypher = "CALL algo.closeness('match (n:weight_cln) return id(n) as id','match (n:weight_cln)-[r:watching|extend]-(n_e:weight_cln) return id(n) as source,id(n_e) as target',{write:true, writeProperty:'closeness',graph:'cypher', concurrency:4})"
        c = carrier()
        c.run_cypher(cypher)
        c.run_cypher(cypher_0)
        print("已计算closeness,并将结果写入节点closeness属性上")
        return cypher


class compute_pagerank(object):
    """
    更新pagerank

    """
    def reset_label(self):
        """
        重置待计算节点

        """
        cypher_0 = "match (n:outside) remove n:weight_pgr;"
        cypher_1 = "match (n:outside:company) set n:weight_pgr;"
        cypher_2 = "match (n:outside:product) set n:weight_pgr;"
        cypher_3 = "match (n:outside:industry) set n:weight_pgr;"
        cypher_4 = "match (n:outside:event) set n:weight_pgr;"
        cypher_5 = "match (n:outside:news) set n:weight_pgr;"
        c = carrier()
        c.run_cypher(cypher_0)
        c.run_cypher(cypher_1)
        c.run_cypher(cypher_2)
        c.run_cypher(cypher_3)
        c.run_cypher(cypher_4)
        c.run_cypher(cypher_5)
        print("已重置待计算标签")
        return cypher_0

    def compute(self):
        """
        计算pagerank

        """
        cypher_0 = "match (n:outside) remove n:weight_pgr;"
        cypher = "CALL algo.pageRank('match (n:weight_pgr) return id(n) as id','match (n:weight_pgr)-[r:watching|extend]-(n_e:weight_pgr) return id(n) as source,id(n_e) as target',{graph:'cypher', iterations:5, write: true});"
        c = carrier()
        c.run_cypher(cypher)
        c.run_cypher(cypher_0)
        print("已计算pagerank,并将结果写入节点pagerank属性上")
        return cypher

class compute_distance(object):  # todo
    """
    更新distance

    """
    def sigmoid(self, x, flag="1"):
        """
        sigmoid归一化(未使用)

        将数值映射到(0,1)区间
        Sigmoid函数是一个具有S形曲线的函数,是良好的阈值函数,在(0,0.5)处中心对称,在(0,0.5)附近有较大斜率,当数据趋向于正无穷和负无穷的时候,映射值会趋向于1和0
        """
        if flag == "1":
            return 1.0/(1+np.exp(-float(x)))
        else:
            return float(x)

    def MaxMinNormalization(x,Max,Min):
        """
        标准归一化

        """
        x = (x - Min) / (Max - Min)
        return x

    def compute(self):          # todo
        """
        计算distance

        """
        cypher = "match ()-[r:watching]->() return r.distance"
        return none


def main():
    """
    程序执行入口

    """
    print('开始计算closeness', datetime.now())
    closeness = compute_closeness()
    closeness.reset_label()
    closeness.compute()
    print('开始计算pagerank', datetime.now())
    pagerank = compute_pagerank()
    pagerank.reset_label()
    pagerank.compute()
    print('已全部执行', datetime.now())

if __name__ == '''__main__''':
    main()
