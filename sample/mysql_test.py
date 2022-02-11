#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-11-03
'''

from mbase.config import MYSQL_CONFIG
from mbase.model import MysqlBaseModel
from mbase.fields import IntField, StringField, DateTimeField, ListField, ObjectField


MYSQL_CONFIG["crucio_insight"] = {
        "host": "localhost",
        "user": "root",
        "password": "12345678",
        "db": "crucio_insight",
        "charset": "utf8mb4",
    }


class User(ObjectField):

    name = StringField()
    age = IntField()


class Address(ObjectField):
    name = StringField()
    num = IntField()


class App(MysqlBaseModel):
    DB_NAME = 'crucio_insight'
    TABLE_NAME = 'app'

    name = StringField(column_mapping=True)
    create_time = DateTimeField()
    update_time = DateTimeField()
    dver = IntField(column_mapping=True)
    user = User()
    address = ListField(item_class=Address)


def save_test():

    user = User(name='hhh', age=12)
    address_1 = Address(name='name_1', num=1)
    address_2 = Address(name='name_2', num=2)
    app = App(name='你猜我猜')
    app.user = user
    # 方式1
    #app.address.append(address_1)
    #app.address.append(address_2)
    # 方式2
    app.address = [address_1, address_2]
    app.save()


def get_test():

    app = App.get('你猜我猜', pk_name='name')
    print(app.__dict__)
    print(app.name, app.user.name)
    print(len(app.address), app.address[0].num)


def f_test():

    app = App()

    address = Address(name='1', num=2)
    print(app.address.items)

    app.address = address

    print(app.address.items)


if __name__ == '__main__':

    # save_test()
    #get_test()
    f_test()

