#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

from typing import Optional

DB_OPTIONS_PARAMS_KEY = [
    "max_versions",
    "compression",
    "in_memory",
    "bloom_filter_type",
    "bloom_filter_vector_size",
    "bloom_filter_nb_hashes",
    "block_cache_enabled",
    "time_to_live",
]


class BaseField(object):
    DATA_TYPE = str

    def __init__(self,
                 name='',
                 family: str = '',
                 # db定义字段
                 max_versions: Optional[int] = None,
                 compression: Optional[str] = None,
                 in_memory: Optional[bool] = None,
                 bloom_filter_type: Optional[str] = None,
                 bloom_filter_vector_size: Optional[int] = None,
                 bloom_filter_nb_hashes: Optional[int] = None,
                 block_cache_enabled: Optional[bool] = None,
                 time_to_live: Optional[int] = None
                 ):
        self.name = name
        self.family = family

        self.max_versions = max_versions
        self.compression = compression
        self.in_memory = in_memory
        self.bloom_filter_type = bloom_filter_type
        self.bloom_filter_vector_size = bloom_filter_vector_size
        self.bloom_filter_nb_hashes = bloom_filter_nb_hashes
        self.block_cache_enabled = block_cache_enabled
        self.time_to_live = time_to_live

    def __unicode__(self):
        if self.family:
            return f"{self.family}:{self.name}"
        else:
            return f"{self.name}:"

    def __get__(self, instance, cls):
        if instance is None:
            return self
        data = instance.__dict__
        return data[self.name]

    def __set__(self, instance, value):
        if isinstance(value, self.__class__.DATA_TYPE):
            instance.__dict__[self.name] = value
            # print(f'set:{self.name}:{value}')
        else:
            print("赋值类型错误", f'set:{self.name}:{value}', self.__class__.DATA_TYPE, type(value))

    def to_python(self, value_str: str = ''):
        # 子类具体实现
        return self.__class__.DATA_TYPE(value_str)

    def db_name(self) -> str:
        name = f'{self.name}:'
        if self.family:
            name = f'{self.family}:{self.name}'
        return name

    def to_db(self, value) -> dict:
        return {self.db_name(): str(value)}


class StringField(BaseField):
    DATA_TYPE = str

    def to_python(self, value_str: str = ''):
        # 子类具体实现
        return self.__class__.DATA_TYPE(value_str)


class IntField(BaseField):
    DATA_TYPE = int


class FloatField(BaseField):
    DATA_TYPE = float

    def to_python(self, raw: str = ''):
        if raw:
            self.value = round(float(raw), 2)
        return self.value


class BaseFamily(object):

    def __init__(self, **attrs):
        if 'family' in attrs:
            self.family = self.name = attrs['family']
        else:
            self.family = self.name = ''
        self.fields = self.__class__.get_fields()
        for attr, attr_obj in self.fields.items():
            attr_obj.family = self.family
            # 没有定义子列名称 使用默认的
            if not attr_obj.name:
                attr_obj.name = attr

        for k, v in attrs.items():
            setattr(self, k, v)

    def __unicode__(self):
        return self.family

    @classmethod
    def get_fields(cls) -> dict:
        fields = {}
        for attr, attr_obj in cls.__dict__.items():
            if not attr.startswith("__") and isinstance(attr_obj, BaseField):
                fields[attr] = attr_obj
        return fields

    def update_family_name(self, family_name):

        self.name = self.family = family_name
        for f, f_obj in self.fields.items():
            f_obj.family = family_name

    def to_python(self, value_dict: dict = {}):
        # 字符串形式转换成定义的形式。子类具体实现
        for k, v in value_dict.items():
            if k in self.fields:
                f_obj = self.fields[k]
                value = f_obj.to_python(v)
                setattr(self, k, value)

    def to_db(self) -> dict:
        # 值转换成字符串形式。供持久化使用
        db_dict = {}
        for k, f_obj in self.fields.items():
            if k in self.__dict__:
                value = self.__dict__[k]
                f_dict = f_obj.to_db(value)
                db_dict.update(f_dict)
        return db_dict