#!/usr/bin/env python
# -*- coding: utf-8 -*
from carrier import *


class compile_model(object):
    """
    解析模型,并返回cypher

    """

    def clean_none(self,value):
        """
        测试用清理none的方法,正式使用时不接受none作参数
        因为产生none的可能性很多,如果不拒绝它,会将问题传染下去

        """
        if value:
            return value
        else:
            value=''
            return value

    # def clean_none(self,value):
    #     """
    #     检测none,并抛出异常

    #     """
    #     if value is None:
    #         raise ValueError('程序拒绝接收空参数')
    #     else:
    #         return value

    def get_model(self):
        """
        获取查询模型

        """
        cypher='''match p=(n:model:active)-[r]->(n_e:model:active) where n.step is not null return n.step as step,n.name as label,n.filt as filt,n.result as result,n.order as order,r.name as relationship, r.direction as direction order by step'''
        c=carrier()
        model=c.run_cypher(cypher).data()
        return model

    def get_cypher(self,model):
        """根据模型构建cypher查询语句

        cypher_tree=f'''match p={path}
        where {where}
        return {result}
        order by {order}
        limit {limit}
        ;'''
        """
        path=''
        where=''
        result=''
        order=''
        limit=''
        # 构建cypher语法
        flag='outside'          # 限定外展标签
        for line in model:
            # print(line)
            # 构建path
            step=line['step']
            label=line['label']
            relationship=line['relationship']
            direction=self.clean_none(line['direction'])
            if direction=='out':
                direction='>'
            elif direction=='in':
                direction='<'
            else:
                direction=direction
            if label=='everything':
                label=f'''(:{flag})'''
            else:
                label=f'''(n_{step}:{flag}:{label})'''
            path=path+label
            if direction=='end':
                relationship=''
            elif relationship=='all':
                relationship=f'''-[*0..1]-{direction}'''
            else:
                relationship=f'''-[:{relationship}]-{direction}'''
            path=path+relationship
            # 构建where
            filt=self.clean_none(line['filt'])
            if filt=='':
                filt=''
            elif where=='':
                filt=filt
            else:
                filt=f''' and {filt}'''
            where=where+filt
            # 构建result
            sub_result=self.clean_none(line['result'])
            if sub_result!='':
                result=result+','+sub_result
            else:
                result=result
            if direction=='end':
                result_node=f'''n_{step}'''
                result=f'''distinct length(p) as len,{result_node}{result}'''
            else:
                result=result
            # 构建order
            sub_order=self.clean_none(line['order'])
            if direction=='end':
                order_default='length(p) asc'
                order=order_default+order
            if sub_order!='':
                order=order+','+sub_order
            else:
                order=order
            # 构建limit
            limit='$limit'
        #构建cypher
        cypher_tree=f'''match p={path}
        where {where}
        return {result}
        order by {order}
        limit {limit}
        ;'''
        cypher_check=f'''match p={path}
        return {result}
        limit 1
        ;'''
        return cypher_tree,cypher_check


def main():
    """
    程序执行入口

    """
    c=compile_model()
    model=c.get_model()
    cypher_list=c.get_cypher(model)
    cypher_tree=cypher_list[0]
    cypher_check=cypher_list[1]
    # 执行check
    c=carrier()
    check=c.run_cypher(cypher_check).data()
    if check:
        cypher=cypher_tree
        print(cypher)
        print(cypher_check)
        return cypher
    else:
        print('自动检查使用cypher:',cypher_check)
        raise ValueError('根据模型配置,无法获取数据')

if __name__=='''__main__''':
    main()
