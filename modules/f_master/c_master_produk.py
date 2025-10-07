from datetime import datetime
import json
from fastapi import HTTPException, Query,Request
from library import *
import os
from library.router import app
from library.db import Db

class c_master_produk(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
        if orderby == None or orderby == '':
            orderby = "id_produk ASC"
        str_clause = self.kendoParse().parse_query(orderby, limit, offset, filter, filter_other, filter_other_conj)
        str_clause_count = self.kendoParse().parse_query("", None, None, filter, filter_other, filter_other_conj)

        sql = f"""SELECT aa.*,
                        ROW_NUMBER() OVER (ORDER BY aa.id_produk DESC) AS nomor_urut,
                        bb.kategori as kategori_name,
                    	cc.id_uom_satuan,
	                    cc.uom_satuan as uom_satuan_name
                FROM master_produk aa 
                LEFT JOIN master_produk_kategori bb ON aa.kategori_produk = bb.id_kategori 
                LEFT JOIN master_produk_uom_satuan cc ON aa.uom_satuan = cc.id_uom_satuan
                """+str_clause

        sql_count = f"""SELECT count(*) as count
                FROM master_produk aa 
                LEFT JOIN master_produk_kategori bb ON aa.kategori_produk = bb.id_kategori 
                LEFT JOIN master_produk_uom_satuan cc ON aa.uom_satuan = cc.id_uom_satuan"""+ str_clause_count
        

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data
    

    async def create(self, data):

        data_kode = await self.get_id_master_kode("PRD")

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "kode_produk": data_kode["kode_produk"],
                "no_urut": data_kode["no_urut"],
            }
        )

        sqlString = self.db.genStrInsertSingleObject(data,"master_produk")

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

        sqlString = self.db.genUpdateObject(data,data_where,"master_produk")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where,"master_produk")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message
    

    async def get_produk(self):
        sql = f"""SELECT aa.id_produk as value,aa.nama_produk||' ('||bb.uom_satuan||')' as text,aa.ppn,aa.pph22 
                  FROM master_produk aa
                  LEFT JOIN master_produk_uom_satuan bb 
                  ON aa.uom_satuan = bb.id_uom_satuan
                  WHERE aa.status_release = 't' AND aa.status_aktif = 't'
                  ORDER BY aa.id_produk ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    
    async def get_produk_where_condition(self,where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']} AND status_release = 't' AND status_aktif = 't'"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT aa.id_produk as value,aa.nama_produk||' ('||bb.uom_satuan||')' as text,aa.ppn,aa.pph22 
    				 FROM master_produk aa {where_sql} 
                     ORDER BY aa.id_produk ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result
    

    async def get_atribut_produk(self, id_produk):
        sql = f"""SELECT id_produk as value,nama_produk as text,* FROM master_produk 
                WHERE id_produk = {id_produk} AND status_release = 't' AND status_aktif = 't'
                LIMIT 1"""
        result = await self.db.executeToDict(sql)
        data = {
            "data": result
        }

        # print(sql)
        return data
    
    async def get_id_master_kode(self,kode_master):


        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM master_produk"""
        
        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        kode_produk = (kode_master+ "." + no_urut[0]["current_no_urut_convert"])
        # print(kode_produk)

        data_kode = {
            "kode_produk": kode_produk,
            "no_urut": no_urut[0]["current_no_urut_convert"],
        }

        return data_kode
    

    async def get_ppn_pph22(self, id_produk):
        sql = f"""SELECT ppn,pph22 FROM master_produk 
                WHERE id_produk = {id_produk} AND status_release = 't' AND status_aktif = 't'
                LIMIT 1"""
        result = await self.db.executeToDict(sql)
        return result[0]


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_produk/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

@app.get("/api/f_master/c_master_produk/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_produk()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_produk/create")
async def create_data(request:Request):
    data = await request.json()
    ob_data = c_master_produk()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_produk/update")
async def update_data(request:Request):
    data = await request.json()
    ob_data = c_master_produk()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_produk/delete")
async def delete(request:Request):
    data = await request.json()
    ob_data = c_master_produk()
    return await ob_data.delete(data)

@app.get("/api/f_master/c_master_produk/get_produk")
async def get_produk():
    ob_data = c_master_produk()
    return await ob_data.get_produk()

@app.get("/api/f_master/c_master_produk/get_produk_where_condition")
async def get_produk_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_produk()
    return await ob_data.get_produk_where_condition(data_where)

@app.get("/api/f_master/c_master_produk/get_atribut_produk")
async def get_atribut_produk(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_produk()
    return await ob_data.get_atribut_produk(data)

@app.get("/api/f_trans/c_holding_inventory_submit/get_id_master_kode")
async def get_id_master_kode( kode_master):
    ob_data = c_master_produk()
    return await ob_data.get_id_master_kode(kode_master)

@app.get("/api/f_master/c_master_produk/get_ppn_pph22")
async def get_ppn_pph22(id_produk):
    ob_data = c_master_produk()
    return await ob_data.get_ppn_pph22(id_produk)



