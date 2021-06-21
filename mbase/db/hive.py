#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2021-03-09
'''

import json
from typing import List

from pyhive import hive


# 运算符字典
OP_DICT = {
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
    'ne': '<>',
}


class HiveConnection(object):

    @classmethod
    def create_client(cls, database: str = ''):
        conn = hive.connect(host='localhost', database=database)
        return conn

    @classmethod
    def format(cls, cursor: hive.Cursor, tuple_items: List[tuple]) -> List[dict]:
        result = []
        columns = []
        for col_info in cursor.description:
            full_name = col_info[0]
            if '.' in full_name:
                col_name = full_name.split('.')[-1]
            else:
                col_name = full_name
            columns.append(col_name)
        for item in tuple_items:
            dict_item = dict(zip(columns, item))
            dict_item['pk'] = None
            result.append(dict_item)
        return result

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

        base_sql = f'select * from {table_name} '
        query_arr = []
        args = []

        for column, value in query_dict.items():
            # 支持 大于 大于等于 小于 小于等于 不等于 运算符
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
                sql = f'{base_sql} where {query_sql} limit %s'
            else:
                sql = f'{base_sql} where {query_sql} limit %s'
        else:
            if desc:
                sql = f'{base_sql} limit %s'
            else:
                sql = f'{base_sql} limit %s'

        args.append(limit)

        with cls.create_client(database=db_name) as conn:
            db = conn.cursor()
            db.execute(sql, tuple(args))
            tuple_items = db.fetchall()
            result = cls.format(db, tuple_items)

        return result

    @classmethod
    def query_count(cls, db_name: str, table_name: str, query_dict: dict) -> int:
        # 分页获取数量
        count = 0

        base_sql = f'select count(1) from {table_name} '
        query_arr = []
        args = []

        for column, value in query_dict.items():
            # 支持运算符
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

        with cls.create_client(database=db_name) as conn:
            db = conn.cursor()
            if query_arr:
                db.execute(sql, tuple(args))
            else:
                db.execute(sql)

            item = db.fetchone()
            count = item[0]
        return count

    @classmethod
    def raw_query(cls, db_name: str, sql: str) -> list:
        # 执行裸查询
        with cls.create_client(database=db_name) as conn:
            db = conn.cursor()
            db.execute(sql)
            tuple_items = db.fetchall()
            result = cls.format(db, tuple_items)
            return result


hive_connect = HiveConnection()


if __name__ == '__main__':

    items = HiveConnection.raw_query('rick', 'select * from account')
    print(items)
