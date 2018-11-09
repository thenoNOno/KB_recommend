#!/usr/bin/env python
# -*- coding: utf-8 -*
from datetime import datetime
import requests


def main():
    """
    程序执行入口

    """
    print('start', datetime.now())
    #content_url = 'http://localhost:9092/api/KB_recommend/assess_content'
    #content_url = 'http://192.168.100.42:9092/api/KB_recommend/assess_content'
    content_url = 'http://192.168.100.42:9090/api/KB_recommend/assess_content'
    data = {'pid':'123', 'content':'银行业发展迅速,华泰证券,石油开采..'}
    content = requests.post(content_url, data)
    print(content.content)
    print(type(content.content))
    print('end', datetime.now())

if __name__ == "__main__":
    main()
