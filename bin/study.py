#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *
from gensim.test.utils import get_tmpfile
from datetime import datetime
import gensim.models as g
import time
import logging


class learn(object):
    """
    使用训练集作为监督,学习实体间关系的权重,完善本体(ontology)的知识图谱

    """
    def __init__(self,train_doc,model_path='1'):
        """
        初始化,训练模型,权重写入图谱

        args:
            train_doc:训练集地址(/test/train_doc.txt).文档字段为[path:[关系],path:[关系]]
            model_path:模型地址(/test/train_test.txt_doc.txt.bin).其他情况['0':初始化模型,'1':默认路径]
        """
        #配置路径
        ru = rule()
        ru.path_exists(train_doc)
        train_doc = train_doc
        save_model_path = train_doc+'.bin'
        save_path = train_doc+'_model.txt'
        model_path = model_path
        #使用w2v纠正维度
        print('开始学习权重:',datetime.now(),'\n使用训练集:',train_doc)
        #配置实例属性,作为训练参数
        self.config()
        #如果model_path参数为1,使用模型存储路径作为传入路径
        if model_path=='1':
            model_path = save_model_path
        else:
            pass
        #如果model_path参数为0,初始化模型,否则使用历史模型,获得更新的模型
        if model_path=='0':
            self.train(train_doc,save_path,save_model_path)
        elif ru.path_exists(model_path):
            self.training(train_doc,model_path,save_path,save_model_path)
        else:
            raise ValueError('模型路径错误')
        self.keep(save_path)
        print('图谱已更新:',datetime.now(),'\n模型路径:',save_model_path)
        #实例属性
        self.train_doc = train_doc
        self.model_path = model_path
        self.save_path = save_path
        self.save_model_path = save_model_path

    def config(self):
        """
        配置训练参数

        """
        #为train()配置参数
        self.size = 300
        #实体预测时上下文仅与相邻实体有关,所以window_size=1
        self.window = 1
        self.min_count = 1
        #使用更低采样率,上下文预测时更大概率跳过常出现的extend,abstract关系
        self.sample = 0.000001
        self.workers = 1
        self.hs = 0
        self.negative = 5
        self.iter = 100
        #配置日志
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)

    def train(self,train_doc,save_path,save_model_path):
        """
        初始化w2v模型

        """
        print('初始化模型')
        docs = g.word2vec.LineSentence(train_doc)
        model = g.Word2Vec(docs,size=self.size
                           ,window=self.window
                           ,min_count=self.min_count
                           ,sample=self.sample
                           ,workers=self.workers
                           ,hs=self.hs
                           ,negative=self.negative
                           ,iter=self.iter)
        #save 模型
        model.save(save_model_path)
        #save word2vec向量与词表
        word_save_path = save_path+'_word.txt'
        model.wv.save_word2vec_format(fname=save_path
                                      ,fvocab=word_save_path
                                      ,binary=False
                                      ,total_vec=None)

    def training(self,train_doc,model_path,save_path,save_model_path):
        """
        加载已有模型,更新词表,增量训练,存储模型文件

        """
        print('加载模型并更新')
        docs = g.word2vec.LineSentence(train_doc)
        model = g.Word2Vec.load(model_path)
        model.build_vocab(docs,update = True)
        model.train(docs, total_examples=model.corpus_count, epochs=model.iter)
        #save 模型
        model.save(save_model_path)
        #save word2vec向量与词表
        word_save_path = save_path+'_word.txt'
        model.wv.save_word2vec_format(fname=save_path
                                      ,fvocab=word_save_path
                                      ,binary=False
                                      ,total_vec=None)

    def keep(self,save_path):
        """
        将向量文件整理格式,写入到图谱中

        """
        #将向量整理为需要的csv格式
        csv_save_path = save_path+'.csv'
        w = writer()
        w.save_vector_csv(save_path,csv_save_path)
        #将csv中的向量更新到neo4j
        print('更新向量到neo4j:',datetime.now(),csv_save_path)
        w.load_vector_neo4j(csv_save_path)


class light(object):
    """
    根据传入的事件数据,从知识图谱中获取特征,生成训练集,存入本地文件

    """
    def __init__(self):
        pass

    def run(self,event_doc,batch='8',path_length='2'):
        """
        初始化,根据事件信息获取图谱路径作为特征,整合为训练集,存入本地文件

        args:
            event_doc:事件地址(/test/event_doc.txt).文档字段为[开始节点,结束节点]
            batch:每个事件的路径特征数量的上限
        """
        #配置路径
        event_doc = event_doc
        train_doc = event_doc+'_doc.txt'
        batch = batch
        ru = rule()
        ru.path_exists(event_doc)
        #如果训练集存在就清理它,重命名到remove_backup路径
        ru.soft_remove(train_doc)
        print('请等待训练集初始化:',datetime.now(),event_doc)
        self.save(event_doc,train_doc,batch,path_length)
        #实例属性
        self.event_doc = event_doc
        self.train_doc = train_doc
        self.batch = batch

    def save(self,event_doc,train_doc,batch,path_length):
        """
        将图谱特征查询的结果作为训练集,存入本地文件

        """
        with open(event_doc,'r',encoding='utf8') as f:
            #next跳过第一行,因为第一行不是事件而是字段名
            next(f)
            for line in f:
                r = rule()
                line = r.line_qualifier(line)
                list = line.replace('\n','').split(' ',1)
                node = list[0]
                node_end = list[1]
                #print(node,node_end)
                thing = self.search(node,node_end,train_doc,batch,path_length)

    def search(self,node,node_end,save_path,batch,path_length):
        """
        查找事件涉及的实体关系

        args:
            node:事件开始节点列表,路径的前二级节点会从该列表中限定,以确保事件开始节点之间可能的watching关系被找出
            node_end:事件结束节点列表
            save_path:事件路径的保存位置
            batch:事件路径的数量
        returns:
            cypher:路径查询使用的cypher
        """
        path_length = path_length
        #filt = f'''id(n) = toInteger('{node}') and id(n_e) = toInteger('{node_end}')'''
        #filt = f''' n.pid in [{node}] and n_e.pid in [{node_end}] '''
        filt = f''' n.pid in [{node}] and n_m.pid in [{node}] and n_e.pid in [{node_end}] '''
        cypher = f'''match p=(n:outside)-[*0..1]->(n_m:outside)-[*0..{path_length}]-(n_e:outside)
        where {filt}
        return timestamp() as batch,extract(x IN relationships(p)|id(x)) as thing,extract(x IN relationships(p)|100.0-toFloat(coalesce(x.norm,0.0))) as distance,length(p) as length
        order by sqrt(coalesce(distance[0],0.0)^2+coalesce(distance[1],0.0)^2+coalesce(distance[2],0.0)^2) asc,length asc
        limit {batch}
        '''
        #print(cypher)
        w = writer()
        thing = w.save_w2v_txt(cypher,save_path)
        return cypher


def main(event_doc='0', train_book='0'):
    """
    程序执行入口

    """
    if train_book == '0':
        event_doc = event_doc
        li = light.run(event_doc,'8')
        train_book == li.train_doc
        le = learn(train_book,'1')
    else:
        learn(train_book,'0')

if __name__=='''__main__''':
    main(event_doc='/KB_recommend/data/train/news_labels.txt', train_book='/KB_recommend/data/train/train_book.txt')
