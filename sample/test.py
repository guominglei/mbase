#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

from mbase.fields import BaseFamily, StringField
from mbase.manager import model_manages
from mbase.model import BaseModel


#################################################
# 类定义
#################################################


class StatInfoFamily(BaseFamily):
    email = StringField()
    phone = StringField()


class Account(BaseModel):
    name = StringField()
    info = StatInfoFamily()

    TABLE_NAME = "account"


#################################################
# 测试
#################################################


def create_test():
    # 初始化表格
    model_manages.init_db()


def save_test():
    # 实例化对象并持久化
    info = StatInfoFamily(email='11212@12.com', phone='1222')
    info.age = 12
    account = Account(name='rick', info=info)
    account.pk = '2020103001'
    account.save()


def get_test():
    # 根据rowkey获取对象
    pk = '2020103001'
    account = Account.get_item_by_pk(pk)
    if account:
        print(account.name, account.info.email, account.info.phone)


def delete_test():
    # 删除
    pk = '2020103001'
    account = Account.get_item_by_pk(pk)
    print(account.info.email)
    if account:
        account.delete(fields=[StatInfoFamily.email])
