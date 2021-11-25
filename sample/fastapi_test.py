#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    @Author  : minglei.guo
    @Contact : minglei@skyplatanus.com
    @Version : 1.0
    @Time    : 2021-11-25
'''

from typing import Optional

import uvicorn
from fastapi import FastAPI, Request

# 打异步补丁
from mbase.patch import patch_model
patch_model()

from mbase.model import MysqlBaseModel
from mbase.fields import StringField, IntField, DateTimeField

# 模型
class App(MysqlBaseModel):

    DB_NAME = 'xx'
    TABLE_NAME = 'app'

    name = StringField(column_mapping=True)
    create_time = DateTimeField()
    update_time = DateTimeField()
    dver = IntField(column_mapping=True)


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Optional[str] = None):
    #return {"item_id": item_id, "q": q}
    print(item_id)
    item = await App.get(pk=int(item_id))
    if item:
        return {'item_id': item.to_db()}
    else:
        return {'error': 'not find'}


@app.post("/items/{item_id}")
async def update_item(item_id: int, request: Request):
    content = await request.json()
    print(content)
    name = content.get('name')
    print(name)
    item = await App.get(pk=int(item_id))
    if item and name:
        item.name = name
        await item.save()
        item = await App.get(pk=int(item_id))
        return {'item_id': item.to_db()}
    else:
        return {'error': 'not find'}


if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=5000, log_level="info")


