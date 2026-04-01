from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db


class c_master_minimum_qty(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == "":
            orderby = "id_price ASC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT aa.*,bb.nama_produk,cc.uom_satuan FROM master_minimum_qty aa 
            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk 
            LEFT JOIN master_produk_uom_satuan cc ON bb.uom_satuan = cc.id_uom_satuan"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) as count FROM master_minimum_qty aa 
            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk 
            LEFT JOIN master_produk_uom_satuan cc ON bb.uom_satuan = cc.id_uom_satuan"""
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

        sqlString = self.db.genStrInsertSingleObject(data, "master_minimum_qty")

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

        sqlString = self.db.genUpdateObject(data, data_where, "master_minimum_qty")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_minimum_qty")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_min_max_qty(self, produk_id, order_type):

        sql = f"""SELECT
        COALESCE(MAX(CASE WHEN min_max = 'max' THEN qty END), 0) AS max_qty,
        COALESCE(MAX(CASE WHEN min_max = 'min' THEN qty END), 0) AS min_qty
        FROM master_minimum_qty
        WHERE produk_id = {produk_id}
        AND status_release = TRUE
        AND status_aktif = TRUE
        AND order_type = '{order_type}';"""

        res = await self.db.executeToDict(sql)
        result = res[0]
        return result

    async def get_minimum_qty(self, produk_id):

        sql = f"""SELECT
            qty 
            FROM
            "public"."master_minimum_qty" 
            WHERE
            produk_id = {produk_id} 
            AND status_release = TRUE 
            AND status_aktif = TRUE"""

        result = await self.db.executeToDict(sql)
        if len(result) == 0:
            return {"minimum_qty": 0}

        qty = result[0]["qty"]
        return {"minimum_qty": qty}
        # print(result)

    async def release(self, data_where):

        sql_unrelease = f"""UPDATE master_minimum_qty SET status_release = 'false'
            (SELECT id_produk,id_cabang,id_company FROM master_minimum_qty 
            WHERE id = '{data_where['id']}') aa
            WHERE master_minimum_qty.id_produk = aa.id_produk"""

        sql_release = f"""UPDATE master_minimum_qty SET status_release = 'true'
            WHERE id = '{data_where['id']}'"""
        try:
            trans = await self.db.executeTrans([sql_unrelease, sql_release])
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("error ketika release sales prices: ", str(e)))

    async def aktif_deaktif(self, data, data_where):
        sqls = []
        sql_aktif = f"""UPDATE master_minimum_qty SET status_aktif = 'false'
            WHERE id = '{data_where['id']}'"""

        if data["status_aktif"] == True:
            sql_unaktif = f"""UPDATE master_minimum_qty SET status_aktif = 'false'
                (SELECT id_produk FROM master_minimum_qty 
                WHERE id = '{data_where['id']}') aa
                WHERE master_minimum_qty.id_produk = aa.id_produk"""

            sqls.append(sql_unaktif)

            sql_aktif = f"""UPDATE master_minimum_qty SET status_aktif = 'true'
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
url/api/c_master_minimum_qty/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_master/c_master_minimum_qty/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_minimum_qty()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_minimum_qty/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_minimum_qty()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_minimum_qty/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_minimum_qty()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_minimum_qty/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_minimum_qty()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_minimum_qty/get_minimum_qty_where_condition")
async def get_minimum_qty_where_condition(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_minimum_qty()
    return await ob_data.get_minimum_qty_where_condition(data_where)


@app.get("/api/f_master/c_master_minimum_qty/get_minimum_qty")
async def get_minimum_qty(id_produk):
    ob_data = c_master_minimum_qty()
    return await ob_data.get_minimum_qty(id_produk)


@app.get("/api/f_master/c_master_minimum_qty/get_min_max_qty")
async def get_min_max_qty(id_produk, order_type):
    ob_data = c_master_minimum_qty()
    return await ob_data.get_min_max_qty(id_produk, order_type)


@app.post("/api/f_master/c_master_minimum_qty/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_master_minimum_qty()
    return await ob_data.release(data["update_where"])


@app.post("/api/f_master/c_master_minimum_qty/aktif_deaktif")
async def aktif_deaktif(request: Request):
    data = await request.json()
    ob_data = c_master_minimum_qty()
    return await ob_data.aktif_deaktif(data["update_data"], data["update_where"])
