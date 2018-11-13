#!/usr/bin/env python
# -*- coding: utf-8 -*
from farm import *


class train():
    """
    对数据进行预测

    """
    def __init__(self, content_doc, event_doc, book):
        self.content_doc = content_doc
        self.event_doc = event_doc
        self.book = book
        self.filepath = os.path.dirname(book)
        print(self.filepath)

    def cast(self, content_doc, event_doc, book):
        #文件路径
        content_doc = content_doc
        event_doc = event_doc
        book = book
        #信息获取
        co = collection_room()
        st = co.apply_worker('stockman')
        st.run(content_doc, event_doc)
        print(st.docs)
        event_doc = st.event_doc
        #并行训练
        co = collection_room()
        le = co.apply_worker('learner')
        le.run(event_doc, book, model_path='1')
        learn_doc = (le.book, le.model_path, le.save_model_path, le.save_path)
        return learn_doc

    def clean(self):
        """
        整理工作目录

        """
        #清理文件夹下临时文件
        co = collection_room()
        clean_place = self.filepath+'/__tmp__'
        co.subtask('cleaner', clean_place, '_tmp_')
        co.subtask('cleaner', clean_place, '_err_')
        co.subtask('cleaner', clean_place, '_term.txt')

    def run(self):
        lock = self.filepath+'/__tmp__'
        todo_place = os.path.exists(self.filepath)
        todo_lock = os.path.exists(lock)
        if todo_place and not todo_lock:
            f = open(lock, 'w')
            f.close()
        else:
            return 'donothing'
        learn_doc = self.cast(self.content_doc, self.event_doc, self.book)
        self.clean()
        return learn_doc

def main():
    """
    程序执行入口

    """
    event_doc = '/KB_recommend/data/train/news_labels.txt'
    content_doc = '/KB_recommend/data/train/content_term_test.txt'
    book = '/KB_recommend/data/train/train_book.txt'
    tr = train(content_doc, event_doc, book)
    learn_doc = tr.run()
    print(learn_doc)

if __name__ == '''__main__''':
    main()
