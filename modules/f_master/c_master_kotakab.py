from datetime import datetime
import json
from fastapi import HTTPException, Query,Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_kotakab(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse


    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
        if orderby == None or orderby == '':
            orderby = "id ASC"
        str_clause = self.kendoParse().parse_query(orderby, limit, offset, filter, filter_other, filter_other_conj)
        str_clause_count = self.kendoParse().parse_query("", None, None, filter, filter_other, filter_other_conj)

        sql = "SELECT *,ROW_NUMBER() OVER (ORDER BY id DESC) AS nomor_urut from master_kotakab"+str_clause
        sql_count = (
            "SELECT count(*) as count FROM master_kotakab"
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

        sqlString = self.db.genStrInsertSingleObject(data,"master_kotakab")

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

        sqlString = self.db.genUpdateObject(data,data_where,"master_kotakab")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where,"master_kotakab")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def get_kotakab(self):
        sql = f"""SELECT id as value,nama as text 
				    FROM master_kotakab ORDER BY id ASC,nama ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    

    #ini yang dipakai untuk insert ke tabel database
    async def get_kotakab_kode(self,kode_prov):
        sql = f"""select kode_kotakab as value, nama as text,trim(kode_prov||kode_kotakab) as kode_concat from master_kotakab
             where kode_prov = trim('{kode_prov}') AND status_release = 't' AND status_aktif = 't'
             ORDER BY nama ASC"""
        
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    


    """
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_kotakab/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_kotakab/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_kotakab()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_kotakab/create")
async def create_data(request:Request):
    data = await request.json()
    ob_data = c_master_kotakab()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_kotakab/update")
async def update_data(request:Request):
    data = await request.json()
    ob_data = c_master_kotakab()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_kotakab/delete")
async def delete(request:Request):
    data = await request.json()
    ob_data = c_master_kotakab()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_kotakab/get_kotakab_kode")
async def get_kotakab_kode(param: object = Query(None, alias="param")):
    data = json.loads(param)
    ob_data = c_master_kotakab()
    return await ob_data.get_kotakab_kode(data)