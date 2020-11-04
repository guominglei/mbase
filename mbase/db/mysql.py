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

import pymysql

from mbase.config import MYSQL_CONFIG


def init_connection() -> dict:
    conn_dict = {}
    for db_name, config in MYSQL_CONFIG.items():
        conn = pymysql.connect(host=config.get('host'),
                               user=config.get('user'),
                               password=config.get('password'),
                               db=config.get('db'),
                               charset=config.get('charset'))
        conn_dict[db_name] = conn
    return conn_dict


class MConnection(object):

    CONN_DICT = {}

    @classmethod
    def get_conn(cls, db_name: str):
        if db_name in cls.CONN_DICT:
            return cls.CONN_DICT[db_name]
        else:
            if db_name in MYSQL_CONFIG:
                config = MYSQL_CONFIG.get(db_name)
                conn = pymysql.connect(host=config.get('host'),
                                       user=config.get('user'),
                                       password=config.get('password'),
                                       db=config.get('db'),
                                       charset=config.get('charset'))
                cls.CONN_DICT[db_name] = conn

                return conn
            else:
                return None

    @classmethod
    def process_in_params(cls, args):
        in_p = ', '.join(list(['%s' for x in args]))
        return in_p

    @classmethod
    def insert(cls, db_name: str, table_name: str, data_dict: dict) -> int:
        # 添加
        conn = cls.get_conn(db_name)
        last_id = -1
        if not conn:
            print('no conn')
            return last_id
        try:
            conn.ping()
            with conn.cursor() as db:
                sql = f'insert into {table_name}(data) value(%s)'
                db.execute(sql, (json.dumps(data_dict),))
                last_id = db.lastrowid
            conn.commit()
        except:
            pass

        return last_id

    @classmethod
    def get_by_pks(cls, db_name: str, table_name: str, pks: List[int], pk_name: str = 'id') -> dict:
        # 根据IDS 获取json信息
        result = {}
        if not db_name or not table_name or not pks:
            return result

        conn = cls.get_conn(db_name)
        if not conn:
            return result

        def _work():
            with conn.cursor() as db:
                sql = f'select id, data from {table_name} where {pk_name} in (%s)'
                sql = sql % (cls.process_in_params(pks),)
                db.execute(sql, pks)
                items = db.fetchall()
                for item in items:
                    pk, raw_data = item
                    json_data = json.loads(raw_data)
                    json_data['pk'] = pk
                    if pk_name == 'id':
                        result[pk] = json_data
                    else:
                        result[json_data[pk_name]] = json_data
        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

        return result

    @classmethod
    def get_by_pk(cls, db_name: str, table_name: str, pk: int, pk_name: str = 'id') -> dict:
        if not db_name or not table_name or not pk:
            return {}
        return cls.get_by_pks(db_name, table_name, pks=[pk, ], pk_name=pk_name).get(pk, {})

    @classmethod
    def update(cls, db_name: str, table_name: str, pk: int, data_dict: dict, dver: int = 0, pk_name: str = 'id') -> int:
        # 修改
        total = 0
        if not db_name or not table_name or not pk or not data_dict:
            return total
        conn = cls.get_conn(db_name)
        if not conn:
            return -1

        def _work():
            with conn.cursor() as db:
                sql = f'update {table_name} set data = %s where {pk_name} = %s'
                # sql = f'update {table_name} set data = %s where {pk_name} = %s and dver = %s'
                raw_data = json.dumps(data_dict)
                db.execute(sql, (raw_data, pk))
                total = db.rowcount
        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

        return total

    @classmethod
    def delete(cls, db_name: str, table_name: str, pk: int, pk_name: str = 'id') -> None:
        # 删除
        conn = cls.get_conn(db_name)
        if not conn:
            return

        def _work():
            with conn.cursor() as db:
                sql = f'delete from {table_name} where {pk_name} = %s'
                db.execute(sql, (pk,))

        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

    @classmethod
    def query(cls,
              db_name: str,
              table_name: str,
              query_dict: dict,
              cursor: int = 0,
              limit: int = 10,
              desc: bool = True,
              pk_name: str = 'id'
              ) -> List[dict]:
        # 分页获取数量
        result = []

        base_sql = f'select id, data from {table_name} '
        query_arr = []
        args = []

        for column, value in query_dict.items():
            if isinstance(value, int):
                if value >= 0:
                    query_arr.append(f'{column}=%s')
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

        conn = cls.get_conn(db_name)
        if not conn:
            return []

        def _work():
            with conn.cursor() as db:
                db.execute(sql, tuple(args))
                items = db.fetchall()
                for pk, raw_data in items:
                    json_data = json.loads(raw_data)
                    json_data['pk'] = pk
                    result.append(json_data)
        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

        return result

    @classmethod
    def query_count(cls, db_name: str, table_name: str, query_dict: dict) -> int:
        # 分页获取数量
        count = 0

        base_sql = f'select count(1) from {table_name} '
        query_arr = []
        args = []

        for column, value in query_dict.items():
            if value >= 0:
                query_arr.append(f'{column}=%s')
                args.append(value)

        if query_arr:
            query_sql = ' and '.join(query_arr)
            sql = f'{base_sql} where {query_sql}'
        else:
            sql = base_sql
        total = 0
        conn = cls.get_conn(db_name)
        if not conn:
            return total

        def _work():
            with conn.cursor() as db:
                if query_arr:
                    db.execute(sql, tuple(args))
                else:
                    db.execute(sql)

                item = db.fetchone()
                count = item[0]
                total = count
        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

        return total

    @classmethod
    def raw_query(cls, db_name: str, sql: str) -> list:
        # 执行裸查询
        result = []
        conn = cls.get_conn(db_name)
        if not conn:
            return result

        def _work():
            with conn.cursor() as db:
                db.execute(sql)
                items = db.fetchall()
                result = items
        try:
            _work()
        except Exception as e:
            if not conn.open:
                conn.ping()
                _work()
            else:
                raise e

        return result


mysql_connect = MConnection()
