from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db


class c_master_user_role(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == '':
            orderby = "updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            "SELECT *,ROW_NUMBER() OVER (ORDER BY id_role DESC) AS nomor_urut from master_user_role"
            + str_clause
        )
        sql_count = "SELECT count(*) as count FROM master_user_role" + str_clause_count

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def create(self, data):
        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlString = self.db.genStrInsertSingleObject(data, "master_user_role")

        try:
            # print(sqlString)
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            # print(e)
            message = {"status": "error : " + str(e)}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def update(self, data, data_where):

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlString = self.db.genUpdateObject(data, data_where, "master_user_role")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_user_role")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_role(self):
        sql = "SELECT id_role as value, role_name as text FROM master_user_role ORDER BY id_role ASC"
        result = await self.db.executeToDict(sql)
        return result

"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_user_role/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_setting/c_master_user_role/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    ob_data = c_master_user_role()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_setting/c_master_user_role/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_user_role()
    return await ob_data.create(data)


@app.post("/api/f_setting/c_master_user_role/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_user_role()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_setting/c_master_user_role/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_user_role()
    return await ob_data.delete(data)


@app.get("/api/f_setting/c_master_user_role/get_role")
async def get_role():
    ob_data = c_master_user_role()
    return await ob_data.get_role()
