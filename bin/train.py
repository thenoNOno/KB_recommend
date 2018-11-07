#!/usr/bin/env python
# -*- coding: utf-8 -*
from farm import *


def main():
    """
    程序执行入口

    """
    #文件路径
    event_doc = '/KB_recommend/data/train/news_labels.txt'
    content_doc = '/KB_recommend/data/train/content_term_test.txt'
    book = '/KB_recommend/data/train/train_book.txt'
    #信息获取
    co = collection_room()
    st = co.apply_worker('stockman')
    st.run(content_doc, event_doc)
    print(st.docs)
    event_doc = st.event_doc
    #并行训练
    co = collection_room()
    le = co.apply_worker('learner')
    le.run(event_doc, book, model_path='0')
    print(le.book, le.model_path, le.save_model_path, le.save_path)
    #清理文件夹下临时文件
    co = collection_room()
    co.subtask('cleaner', content_doc)
    co.subtask('cleaner', book)


if __name__=='''__main__''':
    main()
