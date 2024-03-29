#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-11-03
'''
import json
from typing import List
from asyncio.locks import Lock

import aiomysql
from mbase.config import MYSQL_CONFIG


# 运算符字典
OP_DICT = {
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
    'ne': '<>',
}

INIT_LOCK = Lock()


class AMConnection(object):

    CONN_DICT = {}

    @classmethod
    async def get_pool(cls, db_name: str):
        print('async get_conn')
        if db_name in cls.CONN_DICT:
            pool = cls.CONN_DICT[db_name]
        else:
            if db_name in MYSQL_CONFIG:
                async with INIT_LOCK:
                    if db_name in cls.CONN_DICT:
                        pool = cls.CONN_DICT[db_name]
                    else:
                        config = MYSQL_CONFIG.get(db_name)
                        pool = await aiomysql.pool.create_pool(
                            host=config.get('host'),
                            user=config.get('user'),
                            password=config.get('password'),
                            db=config.get('db'),
                            charset=config.get('charset'),
                            connect_timeout=360,
                        )
                        cls.CONN_DICT[db_name] = pool
            else:
                pool = None

        return pool

    @classmethod
    def release_conn(cls, db_name: str, conn):
        print('release conn')
        if db_name in cls.CONN_DICT:
            pool = cls.CONN_DICT[db_name]
            pool.release(conn)

    @classmethod
    def process_in_params(cls, args):
        in_p = ', '.join(list([f'{x}' for x in args]))
        return in_p

    @classmethod
    async def insert(cls, db_name: str, table_name: str, data_dict: dict) -> int:
        # 添加
        pool = await cls.get_pool(db_name)
        last_id = -1
        if not pool:
            print('no conn')
            return last_id
        try:
            with (await pool) as conn:
                cursor = await conn.cursor()
                sql = f'insert into {table_name}(data) value(%s)'
                await cursor.execute(sql, (json.dumps(data_dict),))
                last_id = cursor.lastrowid
                await conn.commit()
        except:
            pass

        return last_id

    @classmethod
    async def get_by_pks(cls, db_name: str, table_name: str, pks: List[int], pk_name: str = 'id') -> dict:
        # 根据IDS 获取json信息
        result = {}
        if not db_name or not table_name or not pks:
            print(0)
            return result

        pool = await cls.get_pool(db_name)

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                sql = f'select id, data from {table_name} where {pk_name} in (%s)'
                sql = sql % (AMConnection.process_in_params(pks),)
                await cursor.execute(sql)
                items = await cursor.fetchall()
                for item in items:
                    pk, raw_data = item
                    json_data = json.loads(raw_data)
                    json_data['pk'] = pk
                    if pk_name == 'id':
                        result[pk] = json_data
                    else:
                        result[json_data[pk_name]] = json_data

        return result

    @classmethod
    async def get_by_pk(cls, db_name: str, table_name: str, pk: int, pk_name: str = 'id') -> dict:
        if not db_name or not table_name or not pk:
            return {}
        result = await cls.get_by_pks(db_name, table_name, pks=[pk, ], pk_name=pk_name)
        return result.get(pk, {})

    @classmethod
    async def update(cls, db_name: str, table_name: str, pk: int, data_dict: dict, dver: int = 0, pk_name: str = 'id') -> int:
        # 修改
        total = 0
        if not db_name or not table_name or not pk or not data_dict:
            return total
        pool = await cls.get_pool(db_name)
        if not pool:
            return -1

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                sql = f'update {table_name} set data = %s where {pk_name} = %s'
                # sql = f'update {table_name} set data = %s where {pk_name} = %s and dver = %s'
                raw_data = json.dumps(data_dict)
                await cursor.execute(sql, (raw_data, pk))
                total = cursor.rowcount
                await conn.commit()

        return total

    @classmethod
    async def delete(cls, db_name: str, table_name: str, pk: int, pk_name: str = 'id') -> None:
        # 删除
        pool = await cls.get_pool(db_name)
        if not pool:
            return

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                sql = f'delete from {table_name} where {pk_name} = %s'
                await cursor.execute(sql, (pk,))
                await conn.commit()

    @classmethod
    async def query(cls,
              db_name: str,
              table_name: str,
              query_dict: dict,
              cursor: int = 0,
              limit: int = 10,
              desc: bool = True,
              pk_name: str = 'id',
              index_fields: List[str] = [],
              ) -> List[dict]:
        # 分页获取数量
        result = []

        base_sql = f'select id, data from {table_name} '
        query_arr = []
        args = []

        if index_fields:
            # 先组织索引查找条件
            for column in index_fields:
                if column in query_dict:
                    value = query_dict.get(column)
                    query_arr.append(f'{column}=%s')
                    args.append(value)
                    # 查询条件已经用过了。删除
                    query_dict.pop(column)
                else:
                    break

        # 组织普通查找条件
        for column, value in query_dict.items():
            if '__' in column:
                column, op = column.split('__')
                mysql_op = OP_DICT.get(op, None)
                if not mysql_op:
                    continue
                query_arr.append(f'{column}{mysql_op}%s')
                args.append(value)
            else:
                query_arr.append(f'{column}=%s')
                args.append(value)

        if cursor > 0:
            if desc:
                query_arr.append(f'{pk_name} < %s')
            else:
                query_arr.append(f'{pk_name} > %s')
            args.append(cursor)

        if query_arr:
            query_sql = ' and '.join(query_arr)
            if desc:
                sql = f'{base_sql} where {query_sql} order by {pk_name} desc limit %s'
            else:
                sql = f'{base_sql} where {query_sql} limit %s'
        else:
            if desc:
                sql = f'{base_sql} order by {pk_name} desc limit %s'
            else:
                sql = f'{base_sql} limit %s'

        args.append(limit)

        pool = await cls.get_pool(db_name)
        if not pool:
            return []

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, tuple(args))
                items = await cursor.fetchall()
                for pk, raw_data in items:
                    json_data = json.loads(raw_data)
                    json_data['pk'] = pk
                    result.append(json_data)

        return result

    @classmethod
    async def query_count(cls, db_name: str, table_name: str, query_dict: dict, index_fields: List[str] = []) -> int:
        # 分页获取数量
        count = 0

        base_sql = f'select count(1) from {table_name} '
        query_arr = []
        args = []

        if index_fields:
            # 先组织索引查找条件
            for column in index_fields:
                if column in query_dict:
                    value = query_dict.get(column)
                    query_arr.append(f'{column}=%s')
                    args.append(value)
                    # 查询条件已经用过了。删除
                    query_dict.pop(column)
                else:
                    break

        for column, value in query_dict.items():
            if '__' in column:
                column, op = column.split('__')
                mysql_op = OP_DICT.get(op, None)
                if not mysql_op:
                    continue
                query_arr.append(f'{column}{mysql_op}%s')
                args.append(value)
            else:
                query_arr.append(f'{column}=%s')
                args.append(value)

        if query_arr:
            query_sql = ' and '.join(query_arr)
            sql = f'{base_sql} where {query_sql}'
        else:
            sql = base_sql
        total = 0
        pool = await cls.get_pool(db_name)
        if not pool:
            return total

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                if query_arr:
                    await cursor.execute(sql, tuple(args))
                else:
                    await cursor.execute(sql)

                item = await cursor.fetchone()
                count = item[0]
                total = count

        return total

    @classmethod
    async def raw_query(cls, db_name: str, sql: str) -> list:
        # 执行裸查询
        result = []
        pool = await cls.get_pool(db_name)
        if not pool:
            return result

        with (await pool) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql)
                items = await cursor.fetchall()
                result = items

        return result


mysql_connect = AMConnection()
