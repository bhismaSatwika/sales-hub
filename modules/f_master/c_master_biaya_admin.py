from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_biaya_admin(object):
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
            f"""SELECT
                    aa.id_biaya,
                    aa.biaya,
                    aa.status_release,
                    aa.status_aktif,
                    ( CASE WHEN aa.status_release = TRUE THEN 'release' ELSE'draft' END ) AS ket_status_release,
                    cc.id_company AS company_id,
                    cc.company_name,
                    dd.id_cabang AS cabang_id,
                    dd.cabang_name,
                    ROW_NUMBER ( ) OVER ( ORDER BY id_biaya DESC ) AS nomor_urut,
                    cc.id_company,
                    dd.id_cabang,
                    aa.updateindb 
                FROM
                    master_biaya_admin aa
                    LEFT JOIN master_company cc ON aa.id_company = cc.id_company
                    LEFT JOIN master_company_cabang dd ON aa.id_cabang = dd.id_cabang"""
                            + str_clause
                        )

        sql_count = (
            f"""SELECT count(*) as count 
                FROM master_biaya_admin aa
                LEFT JOIN master_company cc ON aa.id_company = cc.id_company
                LEFT JOIN master_company_cabang dd ON aa.id_cabang = dd.id_cabang"""
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

        sqlString = self.db.genStrInsertSingleObject(data, "master_biaya_admin")

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

        sqlString = self.db.genUpdateObject(data, data_where, "master_biaya_admin")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_biaya_admin")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def get_biaya_admin_list(self):
        sql = f"""SELECT id_biaya as value,biaya as text 
				    FROM master_biaya_admin 
                    WHERE status_release = 't' AND status_aktif = 't'
                    ORDER BY id_biaya ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    

    async def get_biaya_admin_where_condition(self, where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']} AND status_release = 't' AND status_aktif = 't'"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT id_biaya as value,biaya as text 
    				FROM master_biaya_admin {where_sql} ORDER BY id_biaya ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    
    async def get_atribut_biaya_admin(self, id_biaya):
        sql = f"""SELECT id_biaya as value,biaya as text,* FROM master_biaya_admin 
                WHERE id_biaya = {id_biaya} AND status_release = 't' AND status_aktif = 't' LIMIT 1"""
        result = await self.db.executeToDict(sql)
        data = {"data": result}

        # print(sql)
        return data
    

    async def get_biaya_admin(self, id_company, id_cabang):
        sql = f"""SELECT biaya FROM master_biaya_admin WHERE id_company = {id_company} AND id_cabang = {id_cabang} AND status_release = 't' AND status_aktif = 't'"""
        try:
            result = await self.db.executeToDict(sql)
            # print(result)
            if len(result) == 0:
                return 0
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
            
        return result[0]["biaya"]
    
    async def release(self,data_where):
        
        sql_unrelease = f"""UPDATE master_biaya_admin SET status_release = 'false'
                (SELECT id_cabang,id_company FROM master_biaya_admin 
                WHERE id_biaya = '{data_where['id_biaya']}') aa
                WHERE master_biaya_admin.id_cabang = aa.id_cabang 
                AND master_biaya_admin.id_company = aa.id_company"""
            
        sql_release = f"""UPDATE master_biaya_admin SET status_release = 'true'
                WHERE id_biaya = '{data_where['id_biaya']}'"""
            
        try:
            trans = await self.db.executeTrans([sql_unrelease,sql_release])
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("error ketika release biaya admin: ", str(e)))
        
    async def aktif_deaktif(self, data,data_where):
        sqls = []
        sql_aktif = f"""UPDATE master_biaya_admin SET status_aktif = 'false'
            WHERE id_biaya = '{data_where['id_biaya']}'"""
        
        if data['status_aktif'] == True:
            sql_unaktif = f"""UPDATE master_biaya_admin SET status_aktif = 'false'
                (SELECT id_cabang,id_company FROM master_biaya_admin 
                WHERE id_biaya = '{data_where['id_biaya']}') aa
                WHERE master_biaya_admin.id_cabang = aa.id_cabang 
                AND master_biaya_admin.id_company = aa.id_company"""
            
            sqls.append(sql_unaktif)
        
            sql_aktif = f"""UPDATE master_biaya_admin SET status_aktif = 'true'
            WHERE id_biaya = '{data_where['id_biaya']}'"""
        
        sqls.append(sql_aktif)
        
        try:
            trans = await self.db.executeTrans([sqls])
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("error ketika aktif biaya admin: ", str(e)))

        
"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_biaya_admin/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_biaya_admin/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_biaya_admin()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_biaya_admin/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_biaya_admin()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_biaya_admin/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_biaya_admin()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_biaya_admin/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_biaya_admin()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_biaya_admin/get_biaya_admin_where_condition")
async def get_biaya_admin_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_biaya_admin()
    return await ob_data.get_biaya_admin_where_condition(data_where)


@app.get("/api/f_master/c_master_biaya_admin/get_atribut_biaya_admin")
async def get_atribut_biaya_admin(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_biaya_admin()
    return await ob_data.get_atribut_biaya_admin(data)


@app.get("/api/f_master/c_master_biaya_admin/get_biaya_admin")
async def get_biaya_admin(id_company, id_cabang):
    ob_data = c_master_biaya_admin()
    return await ob_data.get_biaya_admin(id_company, id_cabang)

@app.post("/api/f_master/c_master_biaya_admin/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_master_biaya_admin()
    return await ob_data.release(data["update_where"])

@app.post("/api/f_master/c_master_biaya_admin/aktif_deaktif")
async def aktif_deaktif(request: Request):
    data = await request.json()
    ob_data = c_master_biaya_admin()
    return await ob_data.aktif_deaktif(data["update_data"], data["update_where"])

