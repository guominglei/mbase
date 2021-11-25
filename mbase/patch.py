#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2021-11-24

    仿gevent 打patch包
    把模型同步方法 更改为 异步方法
    好处就是 方法名称是一致的。
    web 应用 使用 异步方法 提高并发。
    脚本使用同步方法。方便运行。
'''


def patch_model():

    from mbase import asyn_func
    from mbase.db.asyn_mysql import mysql_connect
    from mbase.model import MysqlBaseModel as old_model

    # 需要更改的异步方法
    attrs = ['save', 'delete', 'get_by_pks', 'get', 'get_page_items', 'get_query_count']

    for attr in attrs:

        setattr(old_model, attr, getattr(asyn_func, attr))

    setattr(old_model, 'conn', mysql_connect)