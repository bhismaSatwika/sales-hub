from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db


class c_master_sales_price(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == '':
            orderby = "id_price ASC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT
                    aa.id_price,
                    aa.price,
                    aa.status_release,
                    aa.status_aktif,
                    (CASE 
                    WHEN aa.status_release = true
                        THEN 'release'
                        ELSE 'draft'
                    END) as ket_status_release,
                    bb.id_produk as produk_id,
                    bb.nama_produk,
                    cc.id_company as company_id,
                    cc.company_name,
                    dd.id_cabang as cabang_id,
                    dd.cabang_name,
                    ROW_NUMBER ( ) OVER ( ORDER BY id_price DESC ) AS nomor_urut,
                    bb.id_produk,
                    cc.id_company,
                    dd.id_cabang
                FROM master_sales_price aa
                LEFT JOIN master_produk bb ON aa.id_produk = bb.id_produk
                LEFT JOIN master_company cc ON aa.id_company = cc.id_company
                LEFT JOIN master_company_cabang dd ON aa.id_cabang = dd.id_cabang"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) as count 
                FROM master_sales_price aa
                LEFT JOIN master_produk bb ON aa.id_produk = bb.id_produk
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

        sqlString = self.db.genStrInsertSingleObject(data, "master_sales_price")

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

        sqlString = self.db.genUpdateObject(data, data_where, "master_sales_price")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_sales_price")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_sales_price(self):
        sql = f"""SELECT id_price as value,price as text 
				    FROM master_sales_price 
                    WHERE status_release = 't' AND status_aktif = 't'
                    ORDER BY id_price ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result

    async def get_sales_prices_where_condition(self, where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']} AND status_release = 't' AND status_aktif = 't'"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT id_price as value,price as text 
    				FROM master_sales_price {where_sql} ORDER BY id_price ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result

    async def get_atribut_sales_price(self, id_price):
        sql = f"""SELECT id_price as value,price as text,* FROM master_sales_price 
                WHERE id_price = {id_price} AND status_release = 't' AND status_aktif = 't' LIMIT 1"""
        result = await self.db.executeToDict(sql)
        data = {"data": result}

        # print(sql)
        return data

    async def get_price(self, id_company, id_cabang, id_produk):
        sql = f"""SELECT price FROM master_sales_price WHERE id_company = {id_company} AND id_cabang = {id_cabang} AND id_produk = {id_produk} AND status_release = 't' AND status_aktif = 't'"""
        try:
            result = await self.db.executeToDict(sql)
            # print(result)
            if len(result) == 0:
                return 0
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
            
        return result[0]["price"]
    
    async def release(self, data_where):
    
        sql_unrelease = f"""UPDATE master_sales_price SET status_release = 'false'
            (SELECT id_produk,id_cabang,id_company FROM master_sales_price 
            WHERE id_price = '{data_where['id_price']}') aa
            WHERE master_sales_price.id_produk = aa.id_produk 
            AND master_sales_price.id_cabang = aa.id_cabang 
            AND master_sales_price.id_company = aa.id_company"""
        
        sql_release = f"""UPDATE master_sales_price SET status_release = 'true'
            WHERE id_price = '{data_where['id_price']}'"""
        try:
            trans = await self.db.executeTrans([sql_unrelease,sql_release])
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("error ketika release sales prices: ", str(e)))
        
    
    async def aktif_deaktif(self, data, data_where):
        sqls = []
        sql_aktif = f"""UPDATE master_sales_price SET status_aktif = 'false'
            WHERE id_price = '{data_where['id_price']}'"""
        
        if data['status_aktif'] == True:
            sql_unaktif = f"""UPDATE master_sales_price SET status_aktif = 'false'
                (SELECT id_cabang,id_company FROM master_sales_price 
                WHERE id_price = '{data_where['id_price']}') aa
                WHERE master_sales_price.id_produk = aa.id_produk 
                AND master_sales_price.id_cabang = aa.id_cabang 
                AND master_sales_price.id_company = aa.id_company"""
            
            sqls.append(sql_unaktif)
        
            sql_aktif = f"""UPDATE master_sales_price SET status_aktif = 'true'
            WHERE id_price = '{data_where['id_price']}'"""
        
        sqls.append(sql_aktif)

        try:
            trans = await self.db.executeTrans([sqls])
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("error ketika aktif sales price: ", str(e)))


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_sales_price/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_master/c_master_sales_price/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_sales_price()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_sales_price/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_sales_price()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_sales_price/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_sales_price()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_sales_price/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_sales_price()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_sales_price/get_price_where_condition")
async def get_company_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_sales_price()
    return await ob_data.get_company_where_condition(data_where)


@app.get("/api/f_master/c_master_sales_price/get_atribut_price")
async def get_atribut_price(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_sales_price()
    return await ob_data.get_atribut_price(data)


@app.get("/api/f_master/c_master_sales_price/get_price")
async def get_price(id_company, id_cabang, id_produk):
    ob_data = c_master_sales_price()
    return await ob_data.get_price(id_company, id_cabang, id_produk)


@app.post("/api/f_master/c_master_sales_price/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_master_sales_price()
    return await ob_data.release(data["update_where"])

@app.post("/api/f_master/c_master_sales_price/aktif_deaktif")
async def aktif_deaktif(request: Request):
    data = await request.json()
    ob_data = c_master_sales_price()
    return await ob_data.aktif_deaktif(data["update_data"], data["update_where"])

