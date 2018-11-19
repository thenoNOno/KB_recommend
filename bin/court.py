#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *


class balance():
    """
    评估某实体与某类的实体,两个实体之间发生事件的概率

    """
    def assess_mass(self, node_list, path_length):
        """
        查找实体集合之间的路径,给出每个实体的mass

        根据实体之间路径的权重计算,实体在集合中的重要性
        """
        node_list = node_list
        path_length = path_length
        cypher = f'''
match p=(n)-[]->(n_m)-[*0..{path_length}]-(n_e)
where id(n)<>id(n_e) and id(n) in {node_list} and id(n_e) in {node_list}
with *,extract(x IN relationships(p)|(toFloat(coalesce(x.norm,0.0)))) as mass
return
id(n_e) as n_e,sqrt(sum(coalesce(mass[0],0.0)^2+coalesce(mass[1],0.0)^2)) as mass
order by mass desc
        '''
        c = carrier()
        data = c.run_cypher(cypher).data()
        dataframe = pd.DataFrame(data)
        return dataframe

    def assess_distance(self, pid, node, label_end, path_length, batch):
        """
        查找某实体与类的实体之间的路径,给出两个实体之间的distance

        根据路径权重计算,即实体A与实体之间的关联度
        """
        pid = pid
        node = node
        label_end = label_end
        batch = batch
        filt = f''' n.pid in [{node}] '''
        cypher = f'''match p=(n:outside)-[*0..{path_length}]-(n_e:{label_end})
        where {filt}
        with *,extract(x IN relationships(p)|(100.0-toFloat(coalesce(x.norm,0.0)))) as distances
        return distinct
        n_e.full_name as full_name
        ,{pid} as pid,n.pid as node,n_e.pid as node_end,id(n_e) as n_e,sqrt(sum(coalesce(distances[0],0.0)^2+coalesce(distances[1],0.0)^2+coalesce(distances[2],0.0)^2+coalesce(distances[3],0.0)^2)) as distance,length(p) as length
        order by distance asc
        limit {batch}
        '''
        c = carrier()
        data = c.run_cypher(cypher).data()
        dataframe = pd.DataFrame(data)
        return dataframe

    def assess_chance(self, pid, node, label_end, path_length, batch):
        """
        查找某实体与某类实体中的每一个,相遇的概率

        """
        pid = pid
        node = node
        label_end = label_end
        batch = batch
        distance = self.assess_distance(pid, node, label_end, path_length, batch)
        if distance.empty:
            return distance
        node_list = list(distance['n_e'].drop_duplicates())
        mass = self.assess_mass(node_list, path_length)
        if mass.empty:
            chance = distance.sort_values(by='distance', ascending=True)
        else:
            chance = pd.merge(distance
                              ,mass
                              ,how='left'
                              ,left_on='n_e'
                              ,right_on='n_e')
            chance['chance'] = chance['mass']/chance['distance']
            chance = chance.sort_values(by=['pid','chance','distance']
                                        , ascending=[False,False,True])
        return chance

    def assess(self, nodes, label_end, path_length, batch):
        chances = pd.DataFrame(columns=['pid','node_end','n_e','distance','mass','chance','length'])
        b = balance()
        for line in nodes:
            line = line.split(' ', 1)
            pid = line[0]
            node = line[1]
            chance = b.assess_chance(pid, node, label_end, path_length, batch)
            if chance.empty:
                pass
            else:
                chances = chances.append(chance, ignore_index=True)
        return chances


class snap_algo():
    """
    算法优化

    """
    def fall_next(self, nodes, label_end, batch):
        cypher = f'''match (n)-[r]-(n_e:{label_end})
        where n.pid in {nodes}
        return n_e.pid as n_e, n_e.mass as mass, sum(toFloat(r.norm)) as norm
        order by n_e.mass desc
        limit {batch}
        '''
        print(cypher)
        c = carrier()
        data = c.run_cypher(cypher).data()
        dataframe = pd.DataFrame(data)
        return dataframe

    def fall_step(self, pid, nodes, label_end, path_length, batch):
        step = 0
        tmp_nodes = '['+nodes+']'
        path = pd.DataFrame(columns=['pid','n_e','mass','norm','step'])
        while step < int(path_length):
            tmp_label_end = label_end[step]
            tmp_path = self.fall_next(tmp_nodes, tmp_label_end, batch)
            tmp_path['pid'] = pid
            tmp_path['step'] = step
            if tmp_path.empty:
                return tmp_path
            else:
                tmp_nodes = list(tmp_path['n_e'])
            step = step+1
        path = path.append(tmp_path, ignore_index=True)
        return path

    def fall_stop(self, pid, nodes, label_end, path_length, batch):
        path = self.fall_step(pid, nodes, label_end, path_length, batch)
        if path.empty:
            return path
        else:
            path = path.sort_values(by='norm'
                                    , axis=0
                                    , ascending=False)[0:2]
        return path

    def fall_loop(self, nodes, label_end, path_length, batch):
        chances = pd.DataFrame(columns=['pid','n_e','norm','mass','step'])
        for line in nodes:
            line = line.split(' ', 1)
            pid = line[0]
            node = line[1]
            chance = self.fall_stop(pid, node, label_end, path_length, batch)
            if chance.empty:
                pass
            else:
                chances = chances.append(chance, ignore_index=True)
        return chances


def main():
    """
    程序执行入口

    """

if __name__ == '''__main__''':
    main()
