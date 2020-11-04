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
from mbase.fields import IntField, StringField, DateTimeField


MYSQL_CONFIG["crucio_insight"] = {
        "host": "localhost",
        "user": "root",
        "password": "12345678",
        "db": "crucio_insight",
        "charset": "utf8mb4",
    }


class App(MysqlBaseModel):
    DB_NAME = 'crucio_insight'
    TABLE_NAME = 'app'

    name = StringField(column_mapping=True)
    create_time = DateTimeField()
    update_time = DateTimeField()
    dver = IntField(column_mapping=True)


def save_test():

    app = App(name='你猜我猜')
    app.save()


def get_test():
    import time
    num = 0
    while 1:
        app = App.get('你猜我猜', pk_name='name')
        if app:
            print(app.name, app.create_time)
        time.sleep(30)
        num += 1
        if num == 10:
            break


if __name__ == '__main__':

    # save_test()
    get_test()

