#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

from copy import deepcopy
from typing import Iterable, List, Optional

from mbase.manager import model_manages
from mbase.connect import hb_connection
from mbase.fields import BaseField, BaseFamily


class ModelMeta(type):

    def __new__(cls, name, bases, attrs, **kwargs):
        super_new = super().__new__
        parents = [b for b in bases if isinstance(b, ModelMeta)]
        if not parents:
            return super_new(cls, name, bases, attrs)
        new_class = super_new(cls, name, bases, attrs, **kwargs)
        model_manages.register(new_class.TABLE_NAME, new_class)
        return new_class


class BaseModel(metaclass=ModelMeta):
    TABLE_NAME = ''
    conn = hb_connection

    def __init__(self, **fields):
        # print('init fields', fields)
        self.fields = self.__class__.get_fields()
        # 主键
        self.pk = ''
        # 统计列信息
        for attr, attr_obj in self.fields.items():
            if attr in fields:
                params_attr_obj = fields[attr]
            else:
                params_attr_obj = None
            if isinstance(attr_obj, BaseFamily):
                attr_obj.update_family_name(attr)
                if not params_attr_obj:
                    new_attr_obj = deepcopy(attr_obj)
                else:
                    new_attr_obj = params_attr_obj
                    new_attr_obj.update_family_name(attr)
                self.fields[attr] = new_attr_obj
            elif isinstance(attr_obj, BaseField):
                attr_obj.name = attr
                self.fields[attr] = attr_obj

        for k, v in fields.items():
            setattr(self, k, v)

    def to_python(self, value_dict: dict = {}, is_raw: bool = False):
        # 字符串形式实例化对象
        data = {}
        # 解析原始数据
        if is_raw:
            tmp = {}
            for field_full_name, raw_value in value_dict.items():
                field_full_name = field_full_name.decode('utf-8')
                raw_value = raw_value.decode('utf-8')
                family, field = field_full_name.split(':')
                if family and field:
                    f_info = data.get(family, {})
                    f_info[field] = raw_value
                    tmp[family] = f_info
                else:
                    tmp[family] = raw_value

            value_dict = tmp

        # 各个字段具体格式转换
        for k, v in value_dict.items():
            if k in self.fields:
                f_obj = self.fields[k]
                if isinstance(f_obj, BaseFamily):
                    f_obj.to_python(v)
                    setattr(self, k, f_obj)
                else:
                    value = f_obj.to_python(v)
                    setattr(self, k, value)

    def to_db(self) -> dict:
        # 值转换成字符串形式。供持久化使用
        db_dict = {}
        for k, value in self.__dict__.items():
            f_obj = self.fields.get(k)
            if not f_obj:
                continue
            if isinstance(f_obj, BaseFamily):
                f_dict = f_obj.to_db()
            elif isinstance(f_obj, BaseField):
                value = self.__dict__[k]
                f_dict = f_obj.to_db(value)
            else:
                continue
            db_dict.update(f_dict)
        return db_dict

    def save(self):
        # 保存数据
        if not self.pk:
            print('instance no pk')
            return
        # 持久化
        self.conn.batch_insert(self.__class__.TABLE_NAME, [[self.pk, self.to_db()]])

    def delete(self, fields: List[Optional[BaseField]] = []):
        # 删除整行 或者删除整列。暂不支持列族下的子列
        if not self.pk:
            print('instance no pk')
            return

        columns = []
        if fields:
            for field in fields:
                if isinstance(field, str):
                    columns.append(field)
                else:
                    columns.append(field.db_name())
        is_ok = self.conn.delete(self.__class__.TABLE_NAME, self.pk, columns=columns)
        if not is_ok:
            print('delete error')
        return

    @classmethod
    def generate_pk(cls, *args, **kwargs) -> str:
        # 主键生成规则方法用于生成主键
        # 子类实例化
        return ''

    @classmethod
    def get_fields(cls):
        fields = {}
        for attr, attr_obj in cls.__dict__.items():
            if attr.startswith("__"):
                continue
            if isinstance(attr_obj, BaseFamily):
                # 如果没有指定db里的列 使用默认的
                if not attr_obj.family:
                    attr_obj.update_family_name(attr)
                fields[attr] = attr_obj
            elif isinstance(attr_obj, BaseField):
                # 如果没有指定db里的列 使用默认的
                if not attr_obj.name:
                    attr_obj.name = attr
                fields[attr] = attr_obj

        return fields

    @classmethod
    def generate_table_config(cls) -> dict:
        # 生成数据库表信息
        parma_keys = [
            "max_versions",
            "compression",
            "in_memory",
            "bloom_filter_type",
            "bloom_filter_vector_size",
            "bloom_filter_nb_hashes",
            "block_cache_enabled",
            "time_to_live",
        ]
        table = {}
        for field_name, field_obj in cls.get_fields().items():
            field_config = {}
            for parma_key in parma_keys:
                value = getattr(field_obj, parma_key, None)
                if value is not None:
                    field_config[parma_key] = value
            # 字段名称如果有定义 用定义的。没有就用默认类属性名称。
            table[field_obj.name] = field_config

        return table

    @classmethod
    def create_table(cls) -> bool:
        # 创建表
        is_ok = False

        table_name = cls.TABLE_NAME
        table_config = cls.generate_table_config()
        if not table_name:
            print(f"create:{cls.__name__} table error not set tablename")
            return is_ok
        if not table_config:
            print(f"create:{cls.__name__} table error not generate tableconfig")
            return is_ok

        is_ok = cls.conn.create_table(table_name, table_config)
        return is_ok

    @classmethod
    def batch_insert(cls, data_generate: Iterable, batch_size: int = 10) -> bool:
        # 批量添加
        # data_generate 是一个 rowkey, data_dict 的格式 可遍历的
        is_ok = False

        if not data_generate:
            print('params error')
            return is_ok

        is_ok = cls.conn.batch_insert(cls.TABLE_NAME, data_generate, batch_size=batch_size)

        return is_ok

    @classmethod
    def get_items_by_pks(cls, row_keys: List[str]) -> dict:

        result = {}

        columns = []
        raw_lines_dict = cls.conn.get_items_by_pks(cls.TABLE_NAME, row_keys=row_keys, columns=columns)

        for key, raw_json in raw_lines_dict.items():

            instance = cls()
            instance.to_python(raw_json)
            instance.pk = key
            result[key] = instance

        return result

    @classmethod
    def get_item_by_pk(cls, pk: str = ''):

        if not pk:
            print("no pk")
            return
        return cls.get_items_by_pks([pk, ]).get(pk)
