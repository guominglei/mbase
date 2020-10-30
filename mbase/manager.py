#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

class ModelManager(object):

    def __init__(self):
        self.models = {}

    def register(self, table_name, model_class):
        print(table_name)
        self.models[table_name] = model_class

    def init_db(self):
        for table_name, class_obj in self.models.items():
            print(f'init table:{table_name}')
            class_obj.create_table()


model_manages = ModelManager()
