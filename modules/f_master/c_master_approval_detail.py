from datetime import datetime
import json
from fastapi import HTTPException, Query,Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_approval_detail(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
        if orderby == None or orderby == '':
            orderby = "id_approval ASC"
        str_clause = self.kendoParse().parse_query(orderby, limit, offset, filter, filter_other, filter_other_conj)
        str_clause_count = self.kendoParse().parse_query("", None, None, filter, filter_other, filter_other_conj)

        sql = "SELECT *,ROW_NUMBER() OVER (ORDER BY id_approval DESC) AS nomor_urut from master_approval_detail"+str_clause
        sql_count = (
            "SELECT count(*) as count FROM master_approval_detail"
            + str_clause_count
        )

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

        sqlString = self.db.genStrInsertSingleObject(data,"master_approval_detail")

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

        sqlString = self.db.genUpdateObject(data,data_where,"master_approval_detail")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where,"master_approval_detail")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def get_approval_detail(self):
        sql = f"""SELECT id_approval as value,id_header_approval as text 
				    FROM master_approval_detail ORDER BY id_approval_header ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    
    async def get_approval_detail_where_condition(self,where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']}"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT id_approval_header as value,id_header_approval as text 
    				FROM master_approval_detail {where_sql} ORDER BY id_approval_header ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    

    async def get_atribut_approval_detail(self, id_approval):
        sql = f"""SELECT id_approval_header as value,id_header_approval as text,* FROM master_approval_detail WHERE id_approval = {id_approval} LIMIT 1"""
        result = await self.db.executeToDict(sql)
        data = {
            "data": result
        }

        # print(sql)
        return data


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_approval_detail/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_approval_detail/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_approval_detail()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_approval_detail/create")
async def create_data(request:Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_approval_detail/update")
async def update_data(request:Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_approval_detail/delete")
async def delete(request:Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.delete(data)

@app.get("/api/f_master/c_master_approval_detail/get_approval_detail")
async def get_approval_detail():
    ob_data = c_master_approval_detail()
    return await ob_data.get_approval_detail()


@app.get("/api/f_master/c_master_approval_detail/get_approval_detail_where_condition")
async def get_approval_header_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_approval_detail()
    return await ob_data.get_approval_detail_where_condition(data_where)


@app.get("/api/f_master/c_master_approval_detail/get_atribut_approval_detail")
async def get_atribut_approval_detail(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_approval_detail()
    return await ob_data.get_atribut_approval_detail(data)



