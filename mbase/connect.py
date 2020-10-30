#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2020-10-30
'''

import happybase

HB_POOL = happybase.ConnectionPool(
    size=10,
    host="localhost",
    port=9090,
    timeout=None,
    table_prefix=None,
    table_prefix_separator=b'_',
)
