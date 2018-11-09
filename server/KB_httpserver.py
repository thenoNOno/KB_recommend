#!/usr/bin/env python
# -*- coding: utf-8 -*
from geventwebsocket.handler import WebSocketHandler
from gevent.pywsgi import WSGIServer
from flask import request
from flask import Flask
from datetime import datetime
import os
import time
import json
import pandas as pd


class KBR_server():
    """
    图谱推荐http服务

    """

    app = Flask(__name__)
    def __init__(self, filepath, port, application=app):
        self.filepath = filepath
        self.port = port
        application.config.update(DEBUG=True)
        global SERVER_FILEPATH
        SERVER_FILEPATH = self.filepath
        http_server = WSGIServer(('0.0.0.0', self.port), application, handler_class=WebSocketHandler)
        print('start server:', datetime.now())
        print('server_filepath:', self.filepath)
        print('port:', self.port)
        http_server.serve_forever()

    @app.route('/api/KB_recommend/', methods=['GET', 'POST'])
    def home():
        """
        默认页

        """
        return '<h1>Knowledgegraph base recommend</h1>'

    @app.route("/api/KB_recommend/assess_content", methods=['POST'])
    def assess_content():
        """
        接收文本内容,等待,然后获取分析结果

        """
        if request.method != 'POST':
            raise ValueError('只接收post请求')
        else:
            pass
        try:
            pid = request.form['pid']
            content = request.form['content']
            # 服务的数据处理路径
            do = do_event(SERVER_FILEPATH)
            batch = str(int(time.time()))
            do.put_content(pid, content, batch)
            res = False
            while not res:
                res = do.get_value(pid, batch)
                time.sleep(5)
            code = 200
            status = 'SUCCESS'
        except ValueError:
            code = 403
            status = "FALSE"
        message = {'code':code, 'type':status, 'res':res}
        json_str = json.dumps(message, ensure_ascii=False)
        return json_str

    @app.route("/api/KB_recommend/search_value", methods=['POST'])
    def search_value():
        """
        查询文本term评估结果

        todo
        """
        if request.method != 'POST':
            raise ValueError('只接收post请求')
        else:
            pass
        try:
            pid = request.form['pid']
            code = 200
            status = 'SUCCESS'
        except ValueError:
            code = 403
            status = "FALSE"
        message = {'code':code, 'type':status}
        json_str = json.dumps(message, ensure_ascii=False)
        return json_str


class do_event():
    """
    根据post事件,执行操作

    """
    def __init__(self, server_filepath):
        self.server_filepath = server_filepath

    def put_content(self, pid, content, batch):
        """
        将pid与文本存入本地文件

        """
        content_doc = self.server_filepath+batch+'_content.txt'
        filename = content_doc
        line = pid+' '+content
        with open(filename, mode='a+', encoding='utf8') as f:
            f.write(line)
        return filename

    def get_value(self, pid, batch):
        """
        根据pid查找结果

        """
        value_doc = self.server_filepath+batch+'_content.txt'+'_chances.txt'
        filename = value_doc
        if os.path.exists(filename):
            time.sleep(1)
        else:
            return False
        line = str(pid)
        value_data = pd.read_table(filename, sep=',', encoding='utf8', dtype='str')
        value_data.set_index('pid', drop=False, inplace=True)
        value_data = value_data.loc[value_data.pid==line]
        res = value_data[['pid', 'chance', 'full_name', 'node']].to_dict(orient='records')
        return res


def main():
    """
    程序执行入口

    """
    KBR_server(filepath='/usr/local/KB_recommend/data/forecast/', port=9090)

if __name__ == "__main__":
    main()
