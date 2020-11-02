#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

from typing import Iterable, List

import happybase


class HBConnection(object):

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 9090,
                 table_prefix: str = '',
                 table_prefix_separator: str = '',
                 pool_size: int = 10):

        self.pool = happybase.ConnectionPool(
            size=pool_size,
            host=host,
            port=port,
            timeout=None,
            table_prefix=table_prefix if table_prefix else None,
            table_prefix_separator=table_prefix_separator,
        )

    def create_table(self, table_name: str, table_config: dict) -> bool:
        is_ok = False

        with self.pool.connection() as conn:
            table_list = conn.tables()
            for old_table_name in table_list:
                if old_table_name.decode("utf-8") == table_name:
                    print(f"table:{table_name} is exists")
                    return is_ok

            conn.create_table(table_name, table_config)
            is_ok = True

        return is_ok

    def batch_insert(self, table_name: str, data_iter: Iterable, batch_size: int = 10) -> bool:
        # 批量插入数据
        is_ok = False

        with self.pool.connection() as conn:
            table_list = conn.tables()
            exists_tables = [item.decode('utf-8') for item in table_list]
            if table_name not in exists_tables:
                print("table:{} not is exists")
                return is_ok

            table = conn.table(table_name)
            # 插入数据
            with table.batch(batch_size=batch_size) as batch:
                for key, value_dict in data_iter:
                    print(key, value_dict)
                    batch.put(key, value_dict)

            is_ok = True

        return is_ok

    def get_items_by_pks(self, table_name: str, row_keys: List[str], columns: List[str] = []) -> dict:
        # 根据rowkeys 批量获取数据。 获取的数据是json形式的。model方法自己再实例化对象。
        result = {}

        with self.pool.connection() as conn:
            table_list = conn.tables()
            exists_tables = [item.decode('utf-8') for item in table_list]
            if table_name not in exists_tables:
                print("table:{} not is exists")
                return result

            table = conn.table(table_name)
            rows = table.rows(row_keys, columns=columns)
            for key, value in rows:
                info = {}
                key = key.decode('utf-8')
                for k, v in value.items():
                    k = k.decode('utf-8')
                    v = v.decode('utf-8')
                    parent, child = k.split(':')
                    if child:
                        parent_info = info.get(parent, {})
                        parent_info[child] = v
                        info[parent] = parent_info
                    else:
                        info[parent] = v
                result[key] = info

        return result

    def delete(self, table_name: str, row_key: str, columns: List[str] = []) -> bool:
        is_ok = False

        with self.pool.connection() as conn:
            table_list = conn.tables()
            exists_tables = [item.decode('utf-8') for item in table_list]
            if table_name not in exists_tables:
                print("table:{} not is exists")
                return is_ok

            table = conn.table(table_name)
            if columns:
                print(f"delete:{row_key} colums:{columns}")
                table.delete(row_key, columns=columns)
                is_ok = True
            else:
                print(f"delete:{row_key}")
                table.delete(row_key)
                is_ok = True

        return is_ok

    def scan(self, table_name: str, ):
        pass


hb_connection = HBConnection()