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


class c_inventory_subsidiary_retur_approval(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        company_id=None,
        cabang_id=None,
        username=None,
        filter_other="",
        filter_other_conj="",
    ):

        filter_other = f" zz.username = '{username}' AND zz.id_approval_status_detail=1 and active = true"
        filter_other_conj = f" and "

        if orderby == None or orderby == "":
            orderby = "zz.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT * FROM (
                SELECT 
                    bb.detail_id,
                    bb.order_approve,
                    cc.username,
                    aa.header_id as id_header,
                    aa.approval_status as id_approval_status_header,
                    ee.status_name as approval_status_header,
                    bb.approval_status as id_approval_status_detail,
                    ff.status_name as approval_status_detail,
                    dd.id_invoice,
                    dd.status_release,
                    dd.tanggal_retur,
                    dd.company_id as company_id,
                    hh.company_name,
                    dd.cabang_id as cabang_id,
                    gg.id_trans_sales_order,
                    ii.cabang_name,
                    gg.customer_id,
                    jj.nama_customer,
                    aa.updateindb,
                    bb.active
                FROM trans_approval_header aa
                LEFT JOIN trans_approval_detail bb
                ON aa.header_id = bb.header_id
                LEFT JOIN master_approval cc
                ON bb.master_approval_id = cc.id
                LEFT JOIN trans_inventory_subsidiary_retur_header dd
                ON aa.header_id = dd.id_header
                LEFT JOIN master_approval_status ee
                ON aa.approval_status = ee.id_status
                LEFT JOIN master_approval_status ff
                ON bb.approval_status = ff.id_status
                LEFT JOIN trans_inventory_subsidiary_invoice gg
                ON dd.id_invoice = gg.id_trans
                LEFT JOIN master_company hh
                ON dd.company_id = hh.id_company
                LEFT JOIN master_company_cabang ii
                ON dd.company_id = ii.id_company AND dd.cabang_id = ii.id_cabang
                LEFT JOIN master_customer jj
                ON gg.customer_id = jj.id_customer
            ) zz """
            + str_clause
        )

        sql_count = (
            f"""SELECT COUNT(*) as count FROM (
                SELECT 
                    bb.detail_id,
                    cc.username,
                    aa.header_id as id_trans,
                    aa.approval_status as id_approval_status_header,
                    ee.status_name as approval_status_header,
                    bb.approval_status as id_approval_status_detail,
                    ff.status_name as approval_status_detail,
                    dd.id_invoice,
                    dd.status_release,
                    gg.id_trans_sales_order,
                    dd.tanggal_retur,
                    dd.company_id as company_id,
                    hh.company_name,
                    dd.cabang_id as cabang_id,
                    ii.cabang_name,
                    gg.customer_id,
                    jj.nama_customer,
                    aa.updateindb,
                    bb.active
                FROM trans_approval_header aa
                LEFT JOIN trans_approval_detail bb
                ON aa.header_id = bb.header_id
                LEFT JOIN master_approval cc
                ON bb.master_approval_id = cc.id
                LEFT JOIN trans_inventory_subsidiary_retur_header dd
                ON aa.header_id = dd.id_header
                LEFT JOIN master_approval_status ee
                ON aa.approval_status = ee.id_status
                LEFT JOIN master_approval_status ff
                ON bb.approval_status = ff.id_status
                LEFT JOIN trans_inventory_subsidiary_invoice gg
                ON dd.id_invoice = gg.id_trans
                LEFT JOIN master_company hh
                ON dd.company_id = hh.id_company
                LEFT JOIN master_company_cabang ii
                ON dd.company_id = ii.id_company AND dd.cabang_id = ii.id_cabang
                LEFT JOIN master_customer jj
                ON gg.customer_id = jj.id_customer
            ) zz """
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

        datetime_now = datetime.now()

        # sql_update_status_header = f"""update trans_approval_header
        #     SET approval_status = 1
        #     WHERE header_id = {data["id_retur"]}"""

        sql_update_status = f"""update trans_approval_detail
            SET approval_status = 3, action_time = '{datetime_now}'
            WHERE detail_id = {data["detail_id"]} and active = true"""
        queries.append(sql_update_status)

        sql_update_status_approval_header = f"""UPDATE trans_approval_header hh
                                                    SET approval_status = 3
                                                    WHERE header_id = '{data["id_retur"]}'
                                                    AND NOT EXISTS (
                                                        SELECT approval_status
                                                        FROM trans_approval_detail dd
                                                        WHERE dd.header_id = '{data["id_retur"]}'
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
        WHERE order_approve > {data["order_approve"]} and header_id = '{data["id_retur"]}'
        and active = true
        """

        sql_update_status_approval_header = f"""UPDATE trans_approval_header
                                                    SET approval_status = 4, description = '{data["description"]}', updateindb = '{action_time}'
                                                    WHERE header_id = '{data["id_retur"]}'"""

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


@app.get("/api/f_trans/c_inventory_subsidiary_retur_approval/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    username: str = Query(None, alias="username"),
):
    ob_data = c_inventory_subsidiary_retur_approval()
    return await ob_data.read(
        orderby, limit, offset, filter, company_id, cabang_id, username
    )


@app.post("/api/f_trans/c_inventory_subsidiary_retur_approval/approve")
async def approve(request: Request):
    data = await request.json()
    ob_data = c_inventory_subsidiary_retur_approval()

    return await ob_data.approve(data)


@app.post("/api/f_trans/c_inventory_subsidiary_retur_approval/reject")
async def reject(request: Request):
    data = await request.json()
    ob_data = c_inventory_subsidiary_retur_approval()

    return await ob_data.reject(data)
