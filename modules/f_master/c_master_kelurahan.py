from datetime import datetime
import json
from fastapi import HTTPException, Query,Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_kelurahan(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse


    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
        if orderby == None or orderby == '':
            orderby = "id ASC"
        str_clause = self.kendoParse().parse_query(orderby, limit, offset, filter, filter_other, filter_other_conj)
        str_clause_count = self.kendoParse().parse_query("", None, None, filter, filter_other, filter_other_conj)

        sql = "SELECT *,ROW_NUMBER() OVER (ORDER BY id DESC) AS nomor_urut from master_kelurahan"+str_clause
        sql_count = (
            "SELECT count(*) as count FROM master_kelurahan"
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

        sqlString = self.db.genStrInsertSingleObject(data,"master_kelurahan")

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

        sqlString = self.db.genUpdateObject(data,data_where,"master_kelurahan")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where,"master_kelurahan")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    #ini yang dipakai untuk insert ke tabel database
    async def get_kelurahan_kode(self,kode_prov_kode_kotakab_kode_kec):
        sql = f"""select  kode_kel as value, nama as text from master_kelurahan
              where trim(kode_prov||kode_kotakab||kode_kec) = trim('{kode_prov_kode_kotakab_kode_kec}') 
              AND status_release = 't' AND status_aktif = 't'
              ORDER BY nama ASC"""
        
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    

    """
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_kecamatan/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_kelurahan/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_kelurahan()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_kelurahan/create")
async def create_data(request:Request):
    data = await request.json()
    ob_data = c_master_kelurahan()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_kelurahan/update")
async def update_data(request:Request):
    data = await request.json()
    ob_data = c_master_kelurahan()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_kelurahan/delete")
async def delete(request:Request):
    data = await request.json()
    ob_data = c_master_kelurahan()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_kelurahan/get_kelurahan_kode")
async def get_kelurahan_kode(param: object = Query(None, alias="param")):
    data = json.loads(param)
    ob_data = c_master_kelurahan()
    return await ob_data.get_kelurahan_kode(data)
