#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2021-11-24
'''

import time

from typing import List


async def save(self):
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
            id = await self.conn.insert(self.DB_NAME, self.TABLE_NAME, self.to_db())
            self.id = id
    else:
        # 更新版本用所以不用再实例化为对象。
        raw_dict = await self.conn.get_by_pk(self.DB_NAME, self.TABLE_NAME, self.id)
        if raw_dict:
            # 处理版本信息
            if 'dver' in self.fields:
                old_version = raw_dict.get('dver', 0)
                if old_version is not None:
                    self.dver = old_version + 1 if old_version < 127 else 0
            # 存在更新
            await self.conn.update(self.DB_NAME,
                             self.TABLE_NAME,
                             self.id,
                             self.to_db(),
                             self.PK_NAME)
        else:
            # 插入
            await self.conn.insert(self.DB_NAME, self.TABLE_NAME, self.to_db())


async def delete(self):
    # 删除对象
    if not self.id:
        print('instance no pk')
    else:
        await self.conn.delete(self.__class__.DB_NAME, self.__class__.TABLE_NAME, self.id)
        self.id = None


@classmethod
async def get_by_pks(cls, pk_list: List[str], pk_name: str = '') -> dict:

    result = {}

    if not pk_name:
        pk_name = cls.PK_NAME

    raw_lines_dict = await cls.conn.get_by_pks(cls.DB_NAME, cls.TABLE_NAME, pk_list, pk_name=pk_name)

    for key, raw_json in raw_lines_dict.items():
        instance = cls()
        instance.to_python(raw_json)
        instance.id = raw_json['pk']
        result[key] = instance

    return result


@classmethod
async def get(cls, pk: str = '', pk_name: str = ''):
    if not pk:
        print("no pk")
        return
    result = await cls.get_by_pks([pk, ], pk_name=pk_name)
    return result.get(pk)


@classmethod
async def get_page_items(cls, **filter) -> list:
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

    items = await cls.conn.query(cls.DB_NAME,
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
async def get_query_count(cls, **filter) -> int:
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

    count = await cls.conn.query_count(cls.DB_NAME, cls.TABLE_NAME, query,index_fields=index_fields)
    return count
