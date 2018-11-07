#!/usr/bin/env python
# -*- coding: utf-8 -*
import requests

def main():
    """
    程序执行入口

    """
    #content_url = 'http://localhost:9092/api/KB_recommend/assess_content'
    content_url = 'http://192.168.100.42:9092/api/KB_recommend/assess_content'
    data = {'pid':'123', 'content':'testcontent'}
    content = requests.post(content_url, data)
    print(content.content)

if __name__ == "__main__":
    main()
main()
