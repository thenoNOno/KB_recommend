#!/usr/bin/env python
# -*- coding: utf-8 -*
from farm import *


def main():
    """
    程序执行入口

    """
    #文件路径
    content_doc = '/projects/KB_recommend/test/forecast/content_term_test.txt'
    #信息获取
    co = collection_room()
    st = co.apply_worker('stockman')
    st.run(content_doc)
    print(st.term_doc)
    nodes_doc = st.term_doc
    #并行评估
    co = collection_room()
    ju = co.apply_worker('judge')
    ju.run(nodes_doc, 'entity', '1')
    print(ju.chances_doc, ju.docs)
    #清理文件夹下临时文件
    co = collection_room()
    co.subtask('cleaner', nodes_doc)


if __name__=='''__main__''':
    main()
