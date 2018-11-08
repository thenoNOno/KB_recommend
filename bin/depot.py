#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *
import jieba
dict_term = './dict_term.txt'
jieba.load_userdict(dict_term)


class seeding(object):
    """
    对信息包含的信息子类进行整理

    比如文本分词
    """
    def cut_content(self,filename,target_doc,err_doc):
        c = carrier()
        w = writer()
        data = []
        err_cn = 0
        err_content = []
        #整理term列表
        dict_term_df = pd.read_table(dict_term,sep='\t',encoding='utf8')
        dict_term_df.columns=('term',)
        dict_term_df['term'] = dict_term_df['term'].str.replace('\n', '')
        dict_term_df.set_index('term',drop=False,inplace=True)
        #分词,取term
        with open(filename,'r',encoding='utf8') as f:
            for line in f:
                line = re.sub('(\")','',line)
                columns = str(line).replace('\t',' ').split(' ',1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except:
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content,cut_all=False,HMM=True)
                cut_content = []
                #转换为dataframe使用索引匹配词典
                tmp_line = pd.DataFrame(tmp_cut_content)
                tmp_line.columns=('term',)
                tmp_line.set_index('term',drop=False,inplace=True)
                cut_content = pd.merge(tmp_line,dict_term_df,how="inner",on='term')['term']
                #拼接pid,term
                pid_flag = '\n'+pid+' '
                word_line = pid+' '+pid_flag.join(list(cut_content))+'\n'
                data.append(word_line)
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data,target_doc,100)
        if len(data) != 0:
            c.save_txt(data,target_doc)
        else:
            pass
        c.save_txt(err_content,err_doc)
        return target_doc

    def put_pot(self,filename,target_doc,label,label_end):
        """
        对新闻文本分词获取term,返回dataframe

        """
        term_doc = target_doc
        err_term_doc = target_doc+'_err_.txt'
        tmp_term_doc = filename+'_tmp_.txt'
        c = carrier()
        w = writer()
        tmp_term_doc = self.cut_content(filename,tmp_term_doc,err_term_doc)
        words = []
        err_cn = 0
        err_content = []
        with open(tmp_term_doc,'r',encoding='utf8') as f:
            columns_name = [label,label_end]
            for line in f:
                line = line.split(' ',1)
                try:
                    line_pid = line[0]
                    #正则替换处理特殊符号
                    line_content = re.sub("[\n\s+\.\!\/_,$%^()?;；:-【】+\"\']+|[+——！，;:。？、~@#￥%……&（）]+","",line[1])
                    if line_content!='':
                        words.append((line_pid,line_content))
                    else:
                        pass
                except:
                    err_cn = err_cn+1
                    err_content.append(str(line))
                words = w.save_csv_batch(words,columns_name,term_doc,100)
        if len(words) != 0:
            w.save_csv_batch(words,columns_name,term_doc,0)
        else:
            pass
        c.save_txt(err_content,err_term_doc)
        print('ERROR_log:',err_term_doc,datetime.now())
        return term_doc

    def store_away(self,filename):
        """
        将类拆分子类的结果写入知识图谱

        实体关系入库
        """
        with open(filename,'r',encoding='utf8') as f:
            head = f.readline().split(',',1)
        label = head[0]
        label_end = head[1].replace('\n','')
        check = '{'+f'''pid:row.{label}'''+'}'
        check_e = '{'+f'''pid:row.{label_end}'''+'}'
        batch = '{batchSize:10000, iterateList:false, parallel:false}'
        cypher = f'''
        CALL apoc.periodic.iterate(
        'CALL apoc.load.csv("{filename}") yield map as row return row'
        ,'merge (n:outside:{label} {check}) with *
        merge (n_e:outside:{label_end} {check_e}) with *
        merge (n)-[r:watching]->(n_e) set r.freq = coalesce(r.freq,0)+1,r+=row '
        ,{batch}
        )
        ;
        '''
        c = carrier()
        state = c.run_cypher(cypher)
        return state

    def get_term(self,filename,target_doc):
        c = carrier()
        w = writer()
        err_doc = target_doc+'_err_.txt'
        data = []
        err_cn = 0
        err_content = []
        #整理term列表
        dict_term_df = pd.read_table(dict_term,encoding='utf8')
        dict_term_df.columns=('term',)
        dict_term_df['term'] = dict_term_df['term'].str.replace('\n', '')
        dict_term_df.set_index('term',drop=False,inplace=True)
        #开始分词,取term
        with open(filename,'r',encoding='utf8') as f:
            for line in f:
                line = re.sub('(\")','',line)
                columns = str(line).replace('\t',' ').split(' ',1)
                try:
                    pid = columns[0]
                    content = columns[1]
                except:
                    err_cn = err_cn+1
                    err_content.append(str(columns))
                tmp_cut_content = jieba.cut(content,cut_all=False,HMM=True)
                tmp_cut_content = list(set(tmp_cut_content))
                #转换为dataframe使用索引匹配词典
                tmp_line = pd.DataFrame(tmp_cut_content)
                tmp_line.columns=('term',)
                tmp_line.set_index('term',drop=False,inplace=True)
                cut_content = pd.merge(tmp_line,dict_term_df,how="inner",on='term')['term']
                #拼接pid,term
                if len(cut_content)>0:
                    flag = ','
                    flag_line = flag.join(cut_content)
                    word_line = pid+' '+flag_line+'\n'
                    data.append(word_line)
                else:
                    pass
                # 批量写出,防止分词占用内存过多
                data = w.save_txt_batch(data,target_doc,100)
        if len(data) != 0:
            c.save_txt(data,target_doc)
        else:
            pass
        c.save_txt(err_content,err_doc)
        return target_doc

    def get_event(self,term_doc,event_doc,target_doc):
        c = carrier()
        #整理event_doc
        event_df = pd.read_table(event_doc,encoding='utf8',sep=' ')
        event_df.columns=('n','n_e')
        event_df.set_index('n_e',drop=False,inplace=True)
        #整理term_doc
        term_df = pd.read_table(term_doc,encoding='utf8',sep=' ')
        term_df.columns=('content','term')
        term_df.set_index('content',drop=False,inplace=True)
        #merge
        event_term = pd.merge(event_df,term_df,how="inner",left_on='n_e',right_on='content')[['n','term']]
        event_term.columns=('n','n_e')
        c.save_csv(event_term,target_doc,sep=' ')
        return target_doc


def main():
    """
    程序执行入口

    """

if __name__=='''__main__''':
    main()
