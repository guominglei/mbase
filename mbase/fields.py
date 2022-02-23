#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''
import copy
import time
from typing import Optional
from enum import IntEnum
from datetime import datetime


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
    DEFAULT = None

    def __init__(self,
                 name='',
                 family: str = '',
                 # MySQL 专用字段
                 # 是否是主键
                 is_pk: bool = False,
                 # 是否进行字段映射。 默认不映射。
                 # 没有映射的字段 不能进行字段条件查询
                 column_mapping: bool = False,

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
        self.is_pk = is_pk
        self.column_mapping = column_mapping

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
        # mysql 时 需要处理下。
        if isinstance(instance, BaseFamily):
            if self.name not in instance.__dict__:
                return self.db_name()
        return data.get(self.name, self.DEFAULT)

    def __set__(self, instance, value):
        if isinstance(value, self.__class__.DATA_TYPE):
            instance.__dict__[self.name] = value
            # print(f'set:{self.name}:{value}')
        else:
            print("赋值类型错误", f'set:{self.name}:{value}', self.__class__.DATA_TYPE, type(value))

    def to_python(self, value_str: str = ''):
        # 子类具体实现
        return self.__class__.DATA_TYPE(value_str)

    def db_name(self, is_hb: bool = True) -> str:
        if is_hb:
            # HBASE 字段有上下级关系
            name = f'{self.name}:'
            if self.family:
                name = f'{self.family}:{self.name}'
        else:
            # MySQL字段名称不需要前缀
            name = self.name
        return name

    def to_db(self, value, is_hb: bool = True) -> dict:
        if is_hb:
            return {self.db_name(is_hb): str(value)}
        else:
            return {self.db_name(is_hb): value}


class StringField(BaseField):
    DATA_TYPE = str

    def to_python(self, value_str: str = ''):
        # 子类具体实现
        return self.__class__.DATA_TYPE(value_str)


class IntField(BaseField):
    DATA_TYPE = int
    DEFAULT = 0


class BoolField(BaseField):
    DATA_TYPE = bool
    DEFAULT = False


class FloatField(BaseField):
    DATA_TYPE = float
    DEFAULT = 0

    def to_python(self, raw: str = ''):
        value = 0
        if raw:
            value = round(float(raw), 2)
        return value

    def __set__(self, instance, value):
        if isinstance(value, self.__class__.DATA_TYPE):
            instance.__dict__[self.name] = value
            # print(f'set:{self.name}:{value}')
        else:
            instance.__dict__[self.name] = self.__class__.DATA_TYPE(value)


class DateTimeField(BaseField):
    DATA_TYPE = int
    DEFAULT = 0

    def __get__(self, instance, cls):
        # 返回一个 datetime 对象。
        if instance is None:
            return self
        data = instance.__dict__
        if self.name in data:
            date_time = datetime.fromtimestamp(data.get(self.name)/1000)
        else:
            date_time = datetime.now()
        return date_time

    def __set__(self, instance, value):
        if isinstance(value, self.__class__.DATA_TYPE):
            instance.__dict__[self.name] = value
        elif isinstance(value, datetime):
            instance.__dict__[self.name] = int(time.mktime(value.timetuple()) * 1000)
        else:
            instance.__dict__[self.name] = self.__class__.DATA_TYPE(value)

    @classmethod
    def format(cls, tm_msint: int):
        # 格式化数据
        return time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(tm_msint / 1000)
        )


class ObjectField(BaseField):

    DATA_TYPE = object
    DEFAULT = None

    def __init__(self, **attrs):
        self.name = attrs.get('name', '')
        self.family = ''
        self.is_pk = False
        self.column_mapping = False
        self.fields = self.__class__.get_fields()
        for attr, attr_obj in self.fields.items():
            attr_obj.family = self.family
            # 没有定义子列名称 使用默认的
            if not attr_obj.name:
                attr_obj.name = attr

        for k, v in attrs.items():
            setattr(self, k, v)

    def __unicode__(self):
        return self.name

    @classmethod
    def get_fields(cls) -> dict:
        fields = {}
        for attr, attr_obj in cls.__dict__.items():
            if not attr.startswith("__") and isinstance(attr_obj, BaseField):
                fields[attr] = attr_obj
        return fields

    def to_python(self, value_dict: dict = {}):
        # 字符串形式转换成定义的形式。子类具体实现
        for k, v in value_dict.items():
            if k in self.fields:
                f_obj = self.fields[k]
                value = f_obj.to_python(v)
                setattr(self, k, value)

    def to_db(self, is_hb=False) -> dict:
        # 值转换成字符串形式。供持久化使用
        db_dict = {}
        for k, f_obj in self.fields.items():
            if k in self.__dict__:
                value = self.__dict__[k]
                f_dict = f_obj.to_db(value, is_hb=False)
                db_dict.update(f_dict)
        return db_dict


class ListField(BaseField):
    # 目前只支MySQL

    def __init__(self, name='', family: str = '', item_class=None):
        self.name = name
        self.family = family
        self.is_pk = False
        self.column_mapping = False
        self.item_cls = item_class
        self.items = []

    def __unicode__(self):
        return f"{self.name}:"

    def __set__(self, instance, value):

        if isinstance(value, self.__class__):
            instance.__dict__[self.name] = value

        elif isinstance(value, list):
            tmp_list = []
            if self.item_cls:
                for item in value:
                    if isinstance(item, self.item_cls):
                        tmp_list.append(item)
            else:
                tmp_list = value
            attr = copy.deepcopy(self)
            attr.items = tmp_list
            instance.__dict__[self.name] = attr

        else:
            tmp_list = []
            if self.item_cls:
                if isinstance(value, self.item_cls):
                    tmp_list.append(value)
                else:
                    raise
            else:
                tmp_list.append(value)
            attr = copy.deepcopy(self)
            attr.items = tmp_list
            instance.__dict__[self.name] = attr

    def __get__(self, instance, name):
        if instance is None:
            return self

        if self.name not in instance.__dict__:
            instance.__dict__[self.name] = self.__class__(item_class=self.item_cls)
        data = instance.__dict__

        return data.get(self.name)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def count(self):
        return len(self.items)

    def __len__(self):
        return len(self.items)

    def append(self, item):
        if isinstance(item, self.item_cls):
            self.items.append(item)

    def insert(self, index, item):
        if isinstance(item, self.item_cls):
            self.items.insert(index, item)

    def pop(self, index):
        return self.items.pop(index)

    def clear(self):
        self.items = []

    def to_db(self, is_hb=False) -> dict:
        #
        result = {}
        values = []
        for item in self.items:
            if isinstance(item, ObjectField):
                db_dict = item.to_db(is_hb=False)
                values.append(db_dict)
            else:
                values = self.items

        return values

    def to_python(self, value_list: list = []):
        # 从DB还原成对象
        items = []
        # 处理子对象
        for item in value_list:
            if isinstance(item, dict):
                ins = self.item_cls()
                ins.to_python(value_dict=item)
                items.append(ins)
            else:
                items.append(item)
        self.items = items


class EnumField(BaseField):
    # 只支持MySQL
    DATA_TYPE = int
    DEFAULT = 1

    def __init__(self,
                 name='',
                 family: str = '',
                 # MySQL 专用字段
                 # 是否进行字段映射。 默认不映射。
                 # 没有映射的字段 不能进行字段条件查询
                 column_mapping: bool = False,
                 enum_class: IntEnum =None,
                 ):
        self.name = name
        self.family = family
        self.is_pk = False
        self.column_mapping = column_mapping
        self.enum_class = enum_class

    def __unicode__(self):
        if self.family:
            return f"{self.family}:{self.name}"
        else:
            return f"{self.name}:"

    def __get__(self, instance, cls):
        if instance is None:
            return self
        data = instance.__dict__
        return self.enum_class(data.get(self.name, self.DEFAULT))

    def __set__(self, instance, value):
        if isinstance(value, self.__class__.DATA_TYPE):
            instance.__dict__[self.name] = value
            # print(f'set:{self.name}:{value}')
        else:
            print("赋值类型错误", f'set:{self.name}:{value}', self.__class__.DATA_TYPE, type(value))

    def to_python(self, value_str: str = ''):
        # 子类具体实现
        return self.__class__.DATA_TYPE(value_str)

    def db_name(self, is_hb: bool = True) -> str:
        if is_hb:
            # HBASE 字段有上下级关系
            name = f'{self.name}:'
            if self.family:
                name = f'{self.family}:{self.name}'
        else:
            # MySQL字段名称不需要前缀
            name = self.name
        return name

    def to_db(self, value, is_hb: bool = True) -> dict:
        if is_hb:
            return {self.db_name(is_hb): str(value)}
        else:
            return {self.db_name(is_hb): value}


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

    def to_db(self, is_hb: bool = True) -> dict:
        # 值转换成字符串形式。供持久化使用
        db_dict = {}
        for k, f_obj in self.fields.items():
            if k in self.__dict__:
                value = self.__dict__[k]
                f_dict = f_obj.to_db(value, is_hb=is_hb)
                db_dict.update(f_dict)
        return db_dict


class Index(object):

    def __init__(self, *field_list, index_name: str = ''):

        self.index_name = index_name
        self.field_list = field_list
