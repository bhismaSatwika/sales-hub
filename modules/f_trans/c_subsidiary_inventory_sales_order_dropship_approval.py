from datetime import datetime
import json
import mimetypes
from typing import List, Optional
from fastapi import HTTPException, Query, Request, Form, UploadFile, File
from fastapi.responses import FileResponse
from config import params
from library.router import app
from library.db import Db
from library import *
import os
from modules import f_master
from modules import f_trans
import asyncio


class c_subsidiary_inventory_sales_order_dropship_approval(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        username=None,
        filter_other="",
        filter_other_conj="",
    ):

        filter_other = f"""A.approval_status = 1 
                AND A.active = TRUE 
                AND C.username = '{username}' AND B.order_type = 'dropship' """

        filter_other_conj = f" and "

        if orderby == None or orderby == "":
            orderby = "b.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT
                D.NAME,
                A.*,
                B.* 
                FROM
                trans_approval_detail
                A LEFT JOIN (
                    SELECT
                    aa.ID,
                    aa.order_type,
                    aa.id_trans,
                    aa.no_urut,
                    aa.company_id,
                    aa.cabang_id,
                    aa.salesman,
                    aa.tanggal,
                    aa.customer_id,
                    aa.id_pembayaran,
                    aa.total_ppn,
                    aa.total_pph,
                    aa.harga_total_hpp,
                    aa.biaya_admin,
                    aa.harga_total_ppn_pph,
                    aa.flag_sales_report,
                    aa.status_release,
                    aa.userupdate,
                    aa.updateindb,
                    ee.company_name,
                    aa.harga_total,
                    ff.cabang_name,
                    ( CASE WHEN aa.status_release = TRUE THEN 'release' ELSE'draft' END ) AS ket_status_release,
                    gg.nama_customer,
                    gg.account_va,
                    gg.account_bank_name,
                    hh.md5_file,
                    ii.pembayaran 
                    FROM
                    trans_inventory_subsidiary_sales_order_header aa
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang 
                    AND aa.company_id = ff.id_company
                    LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer
                    LEFT JOIN trans_inventory_subsidiary_invoice hh ON aa.id_trans = hh.id_trans_sales_order
                    LEFT JOIN master_jenis_pembayaran ii ON aa.id_pembayaran = ii.id_pembayaran 
                ) B ON A.header_id = B.id_trans
                LEFT JOIN master_approval C ON A.master_approval_id = C."id"
                LEFT JOIN master_user D ON C.username = D.username 
               """
            + str_clause
        )

        sql_count = (
            f"""SELECT
                count(*)
                FROM
                trans_approval_detail
                A LEFT JOIN (
                    SELECT
                    aa.ID,
                    aa.order_type,
                    aa.id_trans,
                    aa.no_urut,
                    aa.company_id,
                    aa.cabang_id,
                    aa.salesman,
                    aa.tanggal,
                    aa.customer_id,
                    aa.id_pembayaran,
                    aa.total_ppn,
                    aa.total_pph,
                    aa.harga_total_hpp,
                    aa.biaya_admin,
                    aa.harga_total_ppn_pph,
                    aa.flag_sales_report,
                    aa.status_release,
                    aa.userupdate,
                    aa.updateindb,
                    ee.company_name,
                    aa.harga_total,
                    ff.cabang_name,
                    ( CASE WHEN aa.status_release = TRUE THEN 'release' ELSE'draft' END ) AS ket_status_release,
                    gg.nama_customer,
                    gg.account_va,
                    gg.account_bank_name,
                    hh.md5_file,
                    ii.pembayaran 
                    FROM
                    trans_inventory_subsidiary_sales_order_header aa
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang 
                    AND aa.company_id = ff.id_company
                    LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer
                    LEFT JOIN trans_inventory_subsidiary_invoice hh ON aa.id_trans = hh.id_trans_sales_order
                    LEFT JOIN master_jenis_pembayaran ii ON aa.id_pembayaran = ii.id_pembayaran 
                ) B ON A.header_id = B.id_trans
                LEFT JOIN master_approval C ON A.master_approval_id = C."id"
                LEFT JOIN master_user D ON C.username = D.username 
                """
            + str_clause_count
        )

        print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def approve(self, data):

        sql_get_next_id_approval = f"""
                SELECT detail_id FROM trans_approval_detail
            WHERE order_approve > {data["order_approve"]} and active = true
            ORDER BY order_approve asc;
            """

        res_id = await self.db.executeToDict(sql_get_next_id_approval)
        print("res_id", res_id)

        detail_id = None

        queries = []

        if len(res_id) > 0:
            detail_id = res_id[0]["detail_id"]
            sql_update_status_approval_detail = f"""update trans_approval_detail
            SET approval_status = 1
            WHERE detail_id = {detail_id} and active = true"""
            queries.append(sql_update_status_approval_detail)
        else:
            # sql_update_status_approval_detail = f"""update trans_approval_detail
            # SET approval_status = 1
            # WHERE detail_id = {data["detail_id"]} and active = true"""
            # queries.append(sql_update_status_approval_detail)\
            ##TODO release dropship
            print("\n\ntrueeeee\n\n")

        datetime_now = datetime.now()

        # sql_update_status_header = f"""update trans_approval_header
        #     SET approval_status = 1
        #     WHERE header_id = {data["id_retur"]}"""

        sql_update_status = f"""update trans_approval_detail
            SET approval_status = 3, action_time = '{datetime_now}'
            WHERE detail_id = {data["detail_id"]} and active = true"""
        queries.append(sql_update_status)

        sql_update_status_approval_header = f"""UPDATE trans_inventory_subsidiary_sales_order_header hh
                                                    SET approval_status = 3
                                                    WHERE id_trans = '{data["id_trans"]}'
                                                    AND NOT EXISTS (
                                                        SELECT approval_status
                                                        FROM trans_approval_detail dd
                                                        WHERE dd.header_id = '{data["id_trans"]}'
                                                        AND dd.approval_status <> 3 
                                                        AND dd.active = true
                                                    )"""
        queries.append(sql_update_status_approval_header)

        print(queries)

        try:

            res = await self.db.executeTrans(queries)
            if res["status"] == False:
                print(res["detail"])
                raise HTTPException(status_code=400, detail=res["detail"])

            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message

    async def reject(self, data):
        action_time = datetime.now()
        update_description = f"""
                UPDATE trans_approval_detail
        SET description = '{data["description"]}', approval_status = 4
        WHERE detail_id = {data["detail_id"]}  and active = true
"""
        sql_reject = f"""
                UPDATE trans_approval_detail
        SET approval_status = 5, action_time = '{action_time}'
        WHERE order_approve > {data["order_approve"]} and header_id = '{data["id_trans"]}'
        and active = true
        """

        sql_update_status_approval_header = f"""UPDATE trans_inventory_subsidiary_sales_order_header
                                                    SET approval_status = 4, description = '{data["description"]}', updateindb = '{action_time}'
                                                    WHERE id_trans = '{data["id_trans"]}'"""

        print(sql_update_status_approval_header)

        try:
            res = await self.db.executeTrans(
                [update_description, sql_reject, sql_update_status_approval_header]
            )
            if res["status"] == False:
                print(res["detail"])
                raise HTTPException(status_code=400, detail=res["detail"])

            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_inventory_subsidiary_retur_approval/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_subsidiary_inventory_sales_order_dropship_approval/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    username: str = Query(None, alias="username"),
):
    ob_data = c_subsidiary_inventory_sales_order_dropship_approval()
    return await ob_data.read(orderby, limit, offset, filter, username)


@app.post("/api/f_trans/c_subsidiary_inventory_sales_order_dropship_approval/approve")
async def approve(request: Request):
    data = await request.json()
    ob_data = c_subsidiary_inventory_sales_order_dropship_approval()

    return await ob_data.approve(data)


@app.post("/api/f_trans/c_subsidiary_inventory_sales_order_dropship_approval/reject")
async def reject(request: Request):
    data = await request.json()
    ob_data = c_subsidiary_inventory_sales_order_dropship_approval()

    return await ob_data.reject(data)
