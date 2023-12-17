#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-11-02
'''

import time
from copy import deepcopy
from typing import Iterable, List, Optional

from mbase.manager import model_manages
#from mbase.db.hbase import hb_connection
from mbase.db.mysql import mysql_connect
from mbase.fields import BaseField, BaseFamily, EnumField, Index, ObjectField, ListField


class ModelMeta(type):

    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        parents = [b for b in bases if isinstance(b, ModelMeta)]
        if not parents:
            return super_new(cls, name, bases, attrs)
        new_class = super_new(cls, name, bases, attrs)
        model_manages.register(new_class.TABLE_NAME, new_class)
        new_class.fields, new_class.indexes = new_class.get_fields()

        return new_class


class BaseModel(metaclass=ModelMeta):
    # 表名
    TABLE_NAME = ''
    # 连接
    conn = None

    # MYSQL 专用字段
    PK_NAME = 'id'
    # 库名
    DB_NAME = ''
    # 可以查询的字段
    QUERY_FIELDS = []

    def __init__(self, **fields):
        # 主键
        self.id = ''
        # 列信息
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
        if not self.id:
            print('instance no pk')
            return
        # 持久化
        self.conn.batch_insert(self.__class__.TABLE_NAME, [[self.id, self.to_db()]])

    def delete(self, fields: List[Optional[BaseField]] = []):
        # 删除整行 或者删除整列。暂不支持列族下的子列
        if not self.id:
            print('instance no pk')
            return

        columns = []
        if fields:
            for field in fields:
                if isinstance(field, str):
                    columns.append(field)
                else:
                    columns.append(field.db_name())

        is_ok = self.conn.delete(self.__class__.TABLE_NAME, self.id, columns=columns)
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
        for field_name, field_obj in cls.fields.items():
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
            instance.id = key
            result[key] = instance

        return result

    @classmethod
    def get_item_by_pk(cls, pk: str = ''):

        if not pk:
            print("no pk")
            return
        return cls.get_items_by_pks([pk, ]).get(pk)


class MysqlBaseModel(BaseModel):

    # 库名 MYSQL 专用字段
    DB_NAME = ''
    # 表名
    TABLE_NAME = ''
    # MYSQL 专用字段
    PK_NAME = 'id'
    # 连接
    conn = mysql_connect

    def to_python(self, value_dict: dict = {}):
        # 各个字段具体格式转换
        for k, v in value_dict.items():
            if k in self.fields:
                f_obj = self.fields[k]
                if isinstance(f_obj, ObjectField):
                    # 重新初始化一个类对象
                    fb = f_obj.__class__()
                    #fb = deepcopy(f_obj)
                    fb.to_python(v)
                    setattr(self, k, fb)
                elif isinstance(f_obj, ListField):
                    fb = f_obj.__class__(item_class=f_obj.item_cls)
                    fb.to_python(v)
                    setattr(self, k, fb)
                else:
                    value = f_obj.to_python(v)
                    setattr(self, k, value)

    def to_db(self) -> dict:
        # 值转换成字符串形式。供持久化使用
        db_dict = {}
        for k, value in self.__dict__.items():
            f_obj = self.fields.get(k)
            if k not in self.fields:
                continue
            if isinstance(value, ObjectField):
                f_dict = {
                    f_obj.name: value.to_db(is_hb=False)
                }
            elif isinstance(value, ListField):
                f_dict = {
                    f_obj.name: value.to_db(is_hb=False)
                }
            elif isinstance(f_obj, BaseField):
                value = self.__dict__[k]
                f_dict = f_obj.to_db(value, is_hb=False)
            else:
                continue
            db_dict.update(f_dict)
        return db_dict

    def save(self):
        # 保存数据
        # 初始化版本信息
        if 'dver' in self.fields:
            # 描述符 有默认值。判断有无得从__dict__ 来搞
            if 'dver' not in self.__dict__:
                self.dver = 0
        # 添加时间戳
        tm = int(time.time() * 1000)

        if 'create_time' in self.fields and 'create_time' not in self.__dict__:
            self.create_time = tm
        if 'update_time' in self.fields:
            self.update_time = tm

        if not self.id:
            # 没有主键
            if self.PK_NAME != 'id':
                # 主键不是默认的。并且没有值不允许执行
                print('pk is null can not save')
                return
            else:
                # 主键是默认的ID。直接插入
                id = self.conn.insert(self.DB_NAME, self.TABLE_NAME, self.to_db())
                self.id = id
        else:
            # 更新版本用所以不用再实例化为对象。
            raw_dict = self.conn.get_by_pk(self.DB_NAME, self.TABLE_NAME, self.id)
            if raw_dict:
                # 处理版本信息
                if 'dver' in self.fields:
                    old_version = raw_dict.get('dver', 0)
                    if old_version is not None:
                        self.dver = old_version + 1 if old_version < 127 else 0
                # 存在更新
                self.conn.update(self.DB_NAME,
                                 self.TABLE_NAME,
                                 self.id,
                                 self.to_db(),
                                 self.PK_NAME)
            else:
                # 插入
                self.conn.insert(self.DB_NAME, self.TABLE_NAME, self.to_db())

    def delete(self):
        # 删除对象
        if not self.id:
            print('instance no pk')
        else:
            self.conn.delete(self.__class__.DB_NAME, self.__class__.TABLE_NAME, self.id)
            self.id = None

    @classmethod
    def get_fields(cls):
        fields = {}
        indexs = []
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
                # 设置主键名称
                if attr_obj.is_pk:
                    cls.PK_NAME = attr_obj.name
                # 做了字段映射
                if attr_obj.column_mapping:
                    cls.QUERY_FIELDS.append(attr_obj.name)
                fields[attr] = attr_obj
            elif isinstance(attr_obj, Index):
                indexs.append(attr_obj)
        return fields, indexs

    @classmethod
    def generate_table_config(cls) -> dict:
        # 生成数据库表信息
        table = {}
        return table

    @classmethod
    def create_table(cls) -> bool:
        # 创建表
        is_ok = False

        return is_ok

    @classmethod
    def get_by_pks(cls, pk_list: List[str], pk_name: str = '') -> dict:

        result = {}

        if not pk_name:
            pk_name = cls.PK_NAME

        raw_lines_dict = cls.conn.get_by_pks(cls.DB_NAME, cls.TABLE_NAME, pk_list, pk_name=pk_name)

        for key, raw_json in raw_lines_dict.items():
            instance = cls()
            instance.to_python(raw_json)
            instance.id = raw_json['pk']
            result[key] = instance

        return result

    @classmethod
    def get(cls, pk: str = '', pk_name: str = ''):

        if not pk:
            print("no pk")
            return
        return cls.get_by_pks([pk, ], pk_name=pk_name).get(pk)

    @classmethod
    def find_index(cls, query_fields):
        # 按查询条件匹配最长的索引（没有范围选择）
        index_fields = []
        max_length = 0

        if not cls.indexes:
            return index_fields

        for index_obj in cls.indexes:

            for max_len, field in enumerate(index_obj.field_list):
                if field not in query_fields:
                    if max_len > max_length:
                        max_length = max_len
                        index_fields = index_obj.field_list
                        max_len = -1
                    break

            if max_len > 0:
                if not index_fields:
                    index_fields = index_obj.field_list
                else:
                    if max_len > max_length:
                        index_fields = index_obj.field_list

        return index_fields

    @classmethod
    def get_page_items(cls, **filter) -> list:
        # 根据查询条件分页获取数据
        #filter 里需要有
        # cursor: int = 0
        # limit: int = 10
        # desc: bool = True

        result = []
        query = {}
        for field, value in filter.items():
            # 支持运算符
            if '__' in field:
                f, _ = field.split('__')
                if f not in cls.QUERY_FIELDS:
                    continue
                else:
                    query[field] = value
            elif field not in cls.QUERY_FIELDS:
                continue
            else:
                query[field] = value

        # 是否命中索引
        index_fields = cls.find_index(filter)

        cursor = int(filter.get('cursor', 0))
        limit = int(filter.get('limit', 10))
        desc = bool(filter.get('desc', True))

        items = cls.conn.query(cls.DB_NAME,
                               cls.TABLE_NAME,
                               query,
                               cursor=cursor,
                               limit=limit,
                               desc=desc,
                               pk_name=cls.PK_NAME,
                               index_fields=index_fields
                               )

        for item in items:
            pk = item['pk']
            obj = cls()
            obj.to_python(item)
            obj.id = pk
            result.append(obj)

        return result

    @classmethod
    def get_query_count(cls, **filter) -> int:
        count = 0
        query = {}
        for field, value in filter.items():
            # 支持运算符
            if '__' in field:
                f, _ = field.split('__')
                if f not in cls.QUERY_FIELDS:
                    continue
                else:
                    query[field] = value
            elif field not in cls.QUERY_FIELDS:
                continue
            else:
                query[field] = value
        # 是否命中索引
        index_fields = cls.find_index(filter)

        count = cls.conn.query_count(cls.DB_NAME, cls.TABLE_NAME, query,index_fields=index_fields)
        return count

    @classmethod
    def raw_query(cls, sql: str):

        result = cls.conn.raw_query(cls.DB_NAME, sql)
        return result

    @classmethod
    def objects(cls):
        # 遍历所有对象
        cursor = 0
        num = 100
        while 1:
            items = cls.get_page_items(cursor=cursor, limit=num)
            for item in items:
                yield item
            if len(items) < num:
                break
            else:
                cursor += num

    def __getattr__(self, attr_name):

        if attr_name.endswith('_text'):
            attr = attr_name[:-5]
            text_dict_key = f'{attr.upper()}_TEXT'
            text_dict = self.__class__.__dict__[text_dict_key]
            raw_value = self.__dict__.get(attr)
            return text_dict.get(raw_value)