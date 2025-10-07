from datetime import datetime
import json
from fastapi import HTTPException, Query,Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_pajak(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
        if orderby == None or orderby == '':
            orderby = "id_pajak ASC"
        str_clause = self.kendoParse().parse_query(orderby, limit, offset, filter, filter_other, filter_other_conj)
        str_clause_count = self.kendoParse().parse_query("", None, None, filter, filter_other, filter_other_conj)

        sql = f"""SELECT *,ROW_NUMBER() OVER (ORDER BY id_pajak DESC) AS nomor_urut
                FROM master_pajak"""+str_clause

        sql_count = f"""SELECT count(*) as count
                FROM master_produk"""+ str_clause_count
        

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

        sqlString = self.db.genStrInsertSingleObject(data,"master_pajak")

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

        sqlString = self.db.genUpdateObject(data,data_where,"master_pajak")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where,"master_pajak")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def get_pajak(self):
        sql = f"""SELECT presentase as value,pajak as text
                  FROM master_pajak aa
                  ORDER BY id_pajak ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    
    async def get_pajak_where_condition(self,where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']}"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT presentase as value,pajak as text 
    				 FROM master_pajak {where_sql} ORDER BY id_pajak ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    
    async def get_atribut_pajak(self, id_pajak):
        sql = f"""SELECT presentase as value,pajak as text,* FROM master_pajak 
        WHERE id_pajak = {id_pajak} AND status_release = 't' AND status_aktif = 't'
        LIMIT 1"""
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
url/api/c_master_pajak/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_pajak/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_pajak()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_pajak/create")
async def create_data(request:Request):
    data = await request.json()
    ob_data = c_master_pajak()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_pajak/update")
async def update_data(request:Request):
    data = await request.json()
    ob_data = c_master_pajak()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_pajak/delete")
async def delete(request:Request):
    data = await request.json()
    ob_data = c_master_pajak()
    return await ob_data.delete(data)

@app.get("/api/f_master/c_master_pajak/get_pajak")
async def get_pajak():
    ob_data = c_master_pajak()
    return await ob_data.get_pajak()

@app.get("/api/f_master/c_master_pajak/get_pajak_where_condition")
async def get_produk_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_pajak()
    return await ob_data.get_pajak_where_condition(data_where)

@app.get("/api/f_master/c_master_pajak/get_atribut_pajak")
async def get_atribut_pajak(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_pajak()
    return await ob_data.get_atribut_pajak(data)
