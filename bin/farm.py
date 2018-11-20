#!/usr/bin/env python
# -*- coding: utf-8 -*
import threading
from study import *
from court import *
from depot import *


class collection_room():
    """
    工厂类,分配worker

    """
    def __init__(self):
        pass

    def apply_worker(self, worker):
        if worker == 'farmer':
            return farmer()
        elif worker == 'sorter':
            return sorter()
        elif worker == 'learner':
            return learner()
        elif worker == 'rater':
            return rater()
        elif worker == 'brave':
            return brave()
        elif worker == 'judge':
            return judge()
        elif worker == 'cleaner':
            return cleaner()
        elif worker == 'hamaul':
            return hamaul()
        elif worker == 'stockman':
            return stockman()
        elif worker == 'packer':
            return packer()
        elif worker == 'packers':
            return packers()
        else:
            raise ValueError('申请的worker类不存在')

    def subtask(self, worker, doc, target_doc='1', **kwargs):
        work = self.apply_worker(worker)
        if target_doc == '1':
            work.run(doc, **kwargs)
        else:
            work.run(doc, target_doc, **kwargs)
        docs = work.docs
        return docs

    def taskbar(self, worker, docs, target_doc, workload, **kwargs):
        docs_list = []
        p = 0
        t_list = []
        print(worker, ':', workload, datetime.now())
        while p < workload:
            worker = worker
            doc = docs[p]
            t = threading.Thread(target=self.subtask
                                 , args=(worker,doc,target_doc)
                                 , kwargs=kwargs
                                 , name=[worker,doc])
            t.start()
            docs_list.append(doc)
            t_list.append(t)
            p = p+1
        for t in t_list:
            t.join()
        print(worker, ':', workload, datetime.now())
        return docs_list

class worker():
    """
    抽象工作者类

    """
    def __init__(self):
        pass

    def run(self, event_doc, batch):
        pass

    def collect(self, filename, batch):
        pass

    def save(self):
        pass


class cleaner(worker):
    """
    清理传入文件相同路径下含有删除标记的文件,有风险

    """
    def __init__(self):
        pass

    def run(self, filename, delete_flag='_tmp_'):
        delete_flag = delete_flag
        #remove_flag = '.remove'
        cleaned_docs = []
        path, fil = os.path.split(filename)
        for root, dirs, files in os.walk(path, topdown=False):
            file_list = files
        for doc in file_list:
            if doc.rfind(delete_flag) > 0:
                filename = path+'/'+doc
                try:
                    os.remove(filename)
                    cleaned_docs.append(filename)
                except Exception as error:
                    print('文件未清理:', filename, '\n错误信息:', error)
                else:
                    pass
            else:
                pass
        self.cleaned_docs = cleaned_docs
        self.docs = cleaned_docs

    def collect(self, doc):
        pass

    def save(self):
        pass


class farmer(worker):
    """
    根据一个事件集,实例化light,生成一个训练集文件

    """
    def __init__(self):
        pass

    def run(self, event_doc, batch='64', path_length='2'):
        train_doc = self.collect(event_doc, batch, path_length)
        self.train_doc = train_doc
        self.docs = train_doc

    def collect(self, filename, batch, path_length):
        """
        使用light查找事件数据集的特征,生成训练集

        """
        li = light()
        li.run(filename, batch, path_length)
        train_doc = li.train_doc
        return train_doc

    def save(self):
        """
        保存数据集

        """
        pass


class sorter(worker):
    """
    根据数量,切分事件数据集,生成多个子事件集

    """
    def __init__(self):
        pass

    def run(self, event_doc, header=True, batch='5'):
        count = self.collect(event_doc, batch)
        event_docs = self.save(event_doc, count, header)
        self.event_docs = event_docs
        self.docs = event_docs

    def collect(self, filename, batch):
        """
        根据需要batch,计算数据集行数

        """
        batch_size = int(batch)-1
        if batch_size < 1:
            batch_size = 1
        else:
            pass
        line_count = -1
        with open(filename, 'r', encoding='utf8') as f:
            for line_count, line in enumerate(f):
                pass
        line_count = line_count+1
        count = line_count//batch_size
        if count < 1:
            count = 1
        return count

    def save(self, event_doc, count, header):
        """
        保存数据集

        """
        w = writer()
        event_docs = w.split_count(event_doc, count, header)
        return event_docs


class learner(worker):
    """
    learner类,制作工作流并完成模型学习

    """
    def __init__(self):
        pass

    def run(self, doc, book, model_path='1', path_length='2', sorter='sorter', worker='farmer'):
        event_docs = self.collect(doc, path_length, sorter, worker)
        learn = self.save(event_docs, book, model_path)
        self.book = book
        self.model_path = learn.model_path
        self.save_path = learn.save_path
        self.save_model_path = learn.save_model_path

    def collect(self, doc, path_length, sorter, worker):
        print('\n整理任务:', datetime.now())
        co = collection_room()
        event_docs = co.subtask(sorter, doc)
        workload = len(event_docs)
        print('任务量:', workload, '\n任务列表:', event_docs)
        print('开始任务:', datetime.now())
        train_docs = co.taskbar(worker
                                , event_docs
                                , '1'
                                , workload
                                , path_length=path_length)
        print('任务结束:', datetime.now())
        return train_docs

    def save(self, event_docs, book, model_path):
        trains = []
        for event_doc in event_docs:
            train_doc = event_doc+'_doc.txt'
            with open(train_doc) as f:
                lines = f.readlines()
                trains.extend(lines)
        ru = rule()
        ru.soft_remove(book)
        c = carrier()
        c.save_txt(trains, book)
        le = learn(book, model_path)
        return le


class rater(worker):
    """
    预测实体集中每个实体A对应的的类的实体B,A->B的相遇概率

    """
    def __init__(self):
        pass

    def run(self, nodes_doc, label_end, path_length, batch='64'):
        nodes = self.collect(nodes_doc)
        label_end = label_end
        print('寻找label:', label_end)
        b = balance()
        chances = b.assess(nodes, label_end, path_length, batch)
        chances_doc = nodes_doc+'_chances.txt'
        c = carrier()
        c.save_csv(chances, chances_doc)
        self.chances_doc = chances_doc
        self.docs = chances_doc

    def collect(self, filename):
        r = rule()
        nodes = []
        with open(filename, 'r', encoding='utf8') as f:
            next(f)
            for line in f:
                line = r.line_qualifier(line).replace('\n', '')
                nodes.append(line)
        return nodes

    def save(self):
        """
        保存数据集

        """
        pass


class brave(worker):
    """
    使用mass,高效地预测实体集中每个实体A对应的的类的实体B,A->B的相遇概率

    """
    def __init__(self):
        pass

    def run(self, nodes_doc, label_end, path_length, batch='32'):
        nodes = self.collect(nodes_doc)
        label_end = label_end
        print('寻找label:', label_end)
        sn = snap_algo()
        chances = sn.fall_loop(nodes, label_end, path_length, batch)
        chances_doc = nodes_doc+'_chances.txt'
        c = carrier()
        c.save_csv(chances, chances_doc)
        self.chances_doc = chances_doc
        self.docs = chances_doc

    def collect(self, filename):
        r = rule()
        nodes = []
        with open(filename, 'r', encoding='utf8') as f:
            next(f)
            for line in f:
                line = r.line_qualifier(line).replace('\n', '')
                nodes.append(line)
        return nodes

    def save(self):
        """
        保存数据集

        """
        pass


class judge(worker):
    """
    预测实体集与其他实体相遇的概率

    """
    def __init__(self):
        pass

    def run(self, nodes_doc, target_doc, label_end, path_length='1', sorter='sorter', worker='rater'):
        nodes_docs = self.collect(nodes_doc, label_end, path_length, sorter, worker)
        chances_doc = self.save(nodes_docs, target_doc)
        self.chances_doc = chances_doc
        self.docs = chances_doc

    def collect(self, doc, label_end, path_length, sorter, worker):
        print('\n整理任务:', datetime.now())
        co = collection_room()
        nodes_docs = co.subtask(sorter, doc)
        workload = len(nodes_docs)
        print('任务量:', workload, '\n任务列表:', nodes_docs)
        print('开始任务:', datetime.now())
        nodes_docs = co.taskbar(worker
                                , nodes_docs
                                , '1'
                                , workload
                                , label_end=label_end
                                , path_length=path_length)
        print('任务结束:', datetime.now())
        return nodes_docs

    def save(self, nodes_docs, target_doc):
        """
        保存数据集

        """
        chances_docs = []
        for line in nodes_docs:
            chances_doc = line+'_chances.txt'
            chances_docs.append(chances_doc)
        columns = []
        w = writer()
        target_doc = w.merge_csv(chances_docs, target_doc, columns)
        return target_doc


class hamaul(worker):
    """
    生成关系数据,用来产生事件集

    """
    def __init__(self):
        pass

    def run(self, content_doc, term_doc, mode='list'):
        term_doc = self.collect(content_doc, term_doc)
        self.term_doc = term_doc
        self.docs = term_doc

    def collect(self, content_doc, term_doc):
        se = seeding()
        term_doc = se.get_term(content_doc, term_doc)
        return term_doc

    def save(self):
        """
        保存terms数据集

        """
        pass


class stockman(worker):
    """
    并行转换类与子类关系,生成关系集

    subtask切分的batch数越多,执行越快.测试中16G可用内存可以支撑batch=400
    """
    def __init__(self):
        pass

    def run(self, content_doc, event_doc='0',sorter='sorter', worker='hamaul', mode='list', batch='100'):
        term_doc = self.collect(content_doc, mode, sorter, worker, batch)
        if event_doc == '0':
            self.docs = term_doc
        else:
            target_doc = content_doc+'_event_term.txt'
            event_doc = self.save(term_doc, event_doc, target_doc)
            self.event_doc = event_doc
            self.docs = event_doc
        self.term_doc = term_doc

    def collect(self, content_doc, mode, sorter, worker, batch):
        term_doc = content_doc+'_term.txt'
        #创建空的term_doc,写入列名
        term_head = 'pid term\n'
        with open(term_doc, 'w', encoding='utf8') as f:
            f.write(term_head)
        #开始任务
        print('\n整理任务:', datetime.now())
        co = collection_room()
        content_head = 'pid content\n'
        content_docs = co.subtask(sorter
                                  , content_doc
                                  , header=content_head
                                  , batch=batch)
        workload = len(content_docs)
        print('任务量:', workload, '\n任务列表:', content_docs)
        print('开始任务:', datetime.now())
        content_docs = co.taskbar(worker
                                  , content_docs
                                  , term_doc
                                  , workload
                                  , mode=mode)
        print('任务结束:', datetime.now())
        return term_doc

    def save(self, term_doc, event_doc, target_doc):
        """
        保存事件集

        """
        se = seeding()
        target_doc = se.get_event(term_doc, event_doc, target_doc)
        return target_doc


class packer(worker):
    """
    生成关系数据,用来导入图谱

    """
    def __init__(self):
        pass

    def run(self, content_doc, term_doc, mode='list'):
        term_doc = self.collect(content_doc, term_doc, mode)
        self.term_doc = term_doc
        self.docs = term_doc

    def collect(self, content_doc, term_doc, mode):
        se = seeding()
        if mode == 'line':
            term_doc = se.get_full_line_term(content_doc, term_doc)
        elif mode == 'list':
            term_doc = se.get_full_list_term(content_doc, term_doc)
        else:
            pass
        return term_doc

    def save(self):
        """
        保存terms数据集

        """
        pass


class packers(worker):
    """
    并行转换类与子类关系,生成关系集

    subtask切分的batch数越多,执行越快.测试中16G可用内存可以支撑batch=400
    """
    def __init__(self):
        pass

    def run(self, content_doc, sorter='sorter', worker='packer', mode='list', source='0', batch='100', label='content', label_end='term'):
        term_doc = self.collect(content_doc, mode, sorter, worker, batch, label, label_end)
        self.save(term_doc, label, label_end, source)
        self.term_doc = term_doc
        self.docs = term_doc

    def collect(self, content_doc, mode, sorter, worker, batch, label='content', label_end='term'):
        term_doc = content_doc+'_term.txt'
        #创建空的term_doc,写入列名
        term_head = 'pid '+label_end+'\n'
        with open(term_doc, 'w', encoding='utf8') as f:
            f.write(term_head)
        #开始任务
        print('\n整理任务:', datetime.now())
        co = collection_room()
        content_head = 'pid '+label+'\n'
        content_docs = co.subtask(sorter
                                  , content_doc
                                  , header=content_head
                                  , batch=batch)
        workload = len(content_docs)
        print('任务量:', workload, '\n任务列表:', content_docs)
        print('开始任务:', datetime.now())
        content_docs = co.taskbar(worker
                                  , content_docs
                                  , term_doc
                                  , workload
                                  , mode=mode)
        print('任务结束:', datetime.now())
        return term_doc

    def save(self, term_doc, label, label_end, source):
        """
        保存信息到图谱

        """
        if source == '0':
            return 'source is 0'
        else:
            pass
        se = seeding()
        res = se.store_away(term_doc, label, label_end, source)
        print('content_term已导入图谱')


def main():
    """
    程序执行入口

    """

if __name__ == '''__main__''':
    main()
