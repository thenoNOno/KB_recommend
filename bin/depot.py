#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *
import jieba


dict_term = './dict_term.txt'
jieba.load_userdict(dict_term)


class seeding():
    """
    对信息包含的信息子类进行整理

    比如文本分词
    """
    def get_line_term(self, filename, target_doc, err_doc):
        c = carrier()
        w = writer()
        data = []
        err_cn = 0
        err_content = []
        #整理term列表
        dict_term_df = pd.read_table(dict_term, sep='\t', encoding='utf8')
        dict_term_df.columns=('term',)
        dict_term_df['term'] = dict_term_df['term'].str.replace('\n', '')
        dict_term_df.set_index('term', drop=False, inplace=True)
        #分词,取term
        with open(filename, 'r', encoding='utf8') as f:
            for line in f:
                line = re.sub('(\")', '', line)
                columns = str(line).replace('\t', ' ').split(' ', 1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except Exception as error:
                    print('错误信息:', error)
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content, cut_all=False, HMM=True)
                cut_content = []
                #转换为dataframe使用索引匹配词典
                tmp_line = pd.DataFrame(tmp_cut_content)
                tmp_line.columns = ('term',)
                tmp_line.set_index('term', drop=False, inplace=True)
                cut_content = pd.merge(tmp_line
                                       , dict_term_df
                                       , how='inner'
                                       , on='term')['term']
                #拼接pid,term
                pid_flag = '\n'+pid+' '
                word_line = pid+' '+pid_flag.join(list(cut_content))+'\n'
                data.append(word_line)
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data, target_doc, 100)
        if data:
            c.save_txt(data, target_doc)
        else:
            pass
        c.save_txt(err_content, err_doc)
        return target_doc

    def store_away(self, filename, source):
        """
        将类拆分子类的结果写入知识图谱

        实体关系入库
        source:['local':数据文件与neo4j在同一服务器,'mysql':数据文件通过mysql中间库导入neo4j]
        """
        c = carrier()
        w = writer()
        label = 'content'
        label_end = 'term'
        target_doc = filename+'.csv'
        event_df = pd.read_table(filename, encoding='utf8', sep=' ')
        event_df.columns = (label, label_end)
        c.save_csv(event_df, target_doc)
        res = w.load_term_neo4j(target_doc, label, label_end, source)
        return res

    def get_term(self, filename, target_doc):
        c = carrier()
        w = writer()
        err_doc = target_doc+'_err_.txt'
        data = []
        err_cn = 0
        err_content = []
        #整理term列表
        dict_term_df = pd.read_table(dict_term, encoding='utf8')
        dict_term_df.columns = ('term', )
        dict_term_df['term'] = dict_term_df['term'].str.replace('\n', '')
        dict_term_df.set_index('term', drop=False, inplace=True)
        #开始分词,取term
        with open(filename, 'r', encoding='utf8') as f:
            next(f)
            for line in f:
                line = re.sub('(\")', '', line)
                columns = str(line).replace('\t', ' ').split(' ', 1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except Exception as error:
                    print('错误信息:', error)
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content, cut_all=False, HMM=True)
                tmp_cut_content = list(set(tmp_cut_content))
                #转换为dataframe使用索引匹配词典
                tmp_line = pd.DataFrame(tmp_cut_content)
                tmp_line.columns = ('term',)
                tmp_line.set_index('term', drop=False, inplace=True)
                cut_content = pd.merge(tmp_line
                                       , dict_term_df
                                       , how='inner'
                                       , on='term')['term']
                #拼接pid,term
                if cut_content:
                    flag = ','
                    flag_line = flag.join(cut_content)
                    word_line = pid+' '+flag_line+'\n'
                    data.append(word_line)
                else:
                    pass
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data, target_doc, 100)
        if data:
            c.save_txt(data, target_doc)
        else:
            pass
        c.save_txt(err_content, err_doc)
        return target_doc

    def get_full_line_term(self, filename, target_doc):
        c = carrier()
        w = writer()
        err_doc = target_doc+'_err_.txt'
        data = []
        err_cn = 0
        err_content = []
        #开始分词,取term
        with open(filename, 'r', encoding='utf8') as f:
            next(f)
            for line in f:
                line = re.sub('(\")', '', line)
                columns = str(line).replace('\t', ' ').replace('\n', '').split(' ', 1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except Exception as error:
                    print('错误信息:', error)
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content, cut_all=False, HMM=True)
                cut_content = list(set(tmp_cut_content))
                #拼接pid,term
                if cut_content:
                    flag = '\n'+pid+' '
                    word_line = pid+' '+flag.join(cut_content)+'\n'
                    data.append(word_line)
                else:
                    pass
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data, target_doc, 100)
        if data:
            c.save_txt(data, target_doc)
        else:
            pass
        c.save_txt(err_content, err_doc)
        return target_doc

    def get_full_list_term(self, filename, target_doc):
        c = carrier()
        w = writer()
        err_doc = target_doc+'_err_.txt'
        data = []
        err_cn = 0
        err_content = []
        #开始分词,取term
        with open(filename, 'r', encoding='utf8') as f:
            next(f)
            for line in f:
                line = re.sub('(\")', '', line)
                columns = str(line).replace('\t', ' ').replace('\n', '').split(' ', 1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except Exception as error:
                    print('错误信息:', error)
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content, cut_all=False, HMM=True)
                cut_content = list(set(tmp_cut_content))
                if cut_content:
                    flag = ','
                    flag_line = flag.join(cut_content)
                    word_line = pid+' '+flag_line+'\n'
                    data.append(word_line)
                else:
                    pass
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data, target_doc, 100)
        if data:
            c.save_txt(data, target_doc)
        else:
            pass
        c.save_txt(err_content, err_doc)
        return target_doc

    def get_event(self, term_doc, event_doc, target_doc):
        c = carrier()
        #整理event_doc
        event_df = pd.read_table(event_doc, encoding='utf8', sep=' ')
        event_df.columns = ('n', 'n_e')
        event_df.set_index('n_e', drop=False, inplace=True)
        #整理term_doc
        term_df = pd.read_table(term_doc, encoding='utf8', sep=' ')
        term_df.columns = ('content', 'term')
        term_df.set_index('content', drop=False, inplace=True)
        #merge
        event_term = pd.merge(event_df
                              , term_df
                              , how='inner'
                              , left_on='n_e'
                              , right_on='content')[['n', 'term']]
        event_term.columns = ('n', 'n_e')
        c.save_csv(event_term, target_doc, sep=' ')
        return target_doc


def main():
    """
    程序执行入口

    """

if __name__ == '''__main__''':
    main()
