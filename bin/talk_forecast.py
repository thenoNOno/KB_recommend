#!/usr/bin/env python
# -*- coding: utf-8 -*
from farm import *


class forecast():
    """
    对数据进行预测

    """
    def __init__(self, filepath):
        self.filepath = filepath
        timestamp = int(time.time())
        r = rule()
        self.todo_list = r.make_plan(timestamp, filepath, 'content.txt', '.txt')
        print(self.todo_list)

    def cast(self, filename):
        content_doc = filename
        target_doc = content_doc+'_chances.txt'
        #信息获取
        co = collection_room()
        st = co.apply_worker('stockman')
        st.run(content_doc=content_doc, worker='packer', mode='list')
        print(st.term_doc)
        nodes_doc = st.term_doc
        #并行评估
        co = collection_room()
        ju = co.apply_worker('judge')
        ju.run(nodes_doc, target_doc, 'term', path_length='3')
        chances_doc = ju.chances_doc
        return chances_doc

    def clean(self):
        """
        整理工作目录

        """
        #归档2小时前的结果文件
        clean_timestamp = int(time.time())-7200
        r = rule()
        clean_list = r.make_plan(clean_timestamp, self.filepath, 'content.txt_chances.txt', '.txt')
        if clean_list:
            columns = ['pid','node','node_end','n_e','distance','mass','chance','length']
            target_file = self.filepath+'/all_chances.txt'
            w = writer()
            w.merge_csv(clean_list, target_file, columns, mode='a')
            for filename in clean_list:
                os.remove(filename)
        else:
            pass
        #清理文件
        co = collection_room()
        clean_place = self.filepath+'/__tmp__'
        co.subtask('cleaner', clean_place, '_tmp_')
        co.subtask('cleaner', clean_place, '_err_')
        co.subtask('cleaner', clean_place, '_term.txt')

    def run(self):
        todo = self.todo_list
        lock = self.filepath+'/__tmp__'
        todo_place = os.path.exists(self.filepath)
        todo_lock = os.path.exists(lock)
        if todo and todo_place and not todo_lock:
            f = open(lock, 'w')
            f.close()
        else:
            return 'donothing'
        chances_docs = []
        for filename in self.todo_list:
            chances_doc = self.cast(filename)
            chances_docs.append(chances_doc)
            os.remove(filename)
        self.clean()
        return chances_docs

def main():
    """
    程序执行入口

    """
    fo = forecast('/KB_recommend/test/forecast')
    chances_docs = fo.run()
    print(chances_docs)

if __name__ == '''__main__''':
    main()
