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

    async def read(self,orderby,limit,offset,filter,company_id=None,cabang_id=None,username=None,filter_other="",filter_other_conj=""):
        if company_id != None and cabang_id != None:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}' AND zz.username = '{username}' AND zz.id_approval_status_detail=1"
            )
            filter_other_conj = f" and "

            if company_id == 1:
                filter_other = f""
                filter_other_conj = f""

            if company_id == 2 and cabang_id == 11:
                filter_other = f" zz.company_id = '{company_id}'"

        else:
            filter_other = f""
            filter_other_conj = f""

        if orderby == None or orderby == "":
            orderby = "zz.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = f"""SELECT * FROM (
                SELECT 
                    cc.username,
                    aa.header_id as id_retur,
                    aa.approval_status as id_approval_status_header,
                    ee.status_name as approval_status_header,
                    bb.approval_status as id_approval_status_detail,
                    ff.status_name as approval_status_detail,
                    dd.id_invoice,
                    dd.tanggal_retur,
                    dd.company_id as company_id,
                    hh.company_name,
                    dd.cabang_id as cabang_id,
                    ii.cabang_name,
                    gg.customer_id,
                    jj.nama_customer
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
            ) zz """+str_clause


        sql_count = f"""SELECT (*) as count FROM (
                SELECT 
                    cc.username,
                    aa.header_id as id_retur,
                    aa.approval_status as id_approval_status_header,
                    ee.status_name as approval_status_header,
                    bb.approval_status as id_approval_status_detail,
                    ff.status_name as approval_status_detail,
                    dd.id_invoice,
                    dd.tanggal_retur,
                    dd.company_id as company_id,
                    hh.company_name,
                    dd.cabang_id as cabang_id,
                    ii.cabang_name,
                    gg.customer_id,
                    jj.nama_customer
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
            ) zz """+str_clause_count

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data
    

    async def approve(self, data):        

        sql_update_status_approval_detail = f"""UPDATE trans_approval_detail
                                                    SET approval_status = CASE
                                                        WHEN approval_status = 1 THEN 3
                                                        WHEN approval_status = 2 THEN 1
                                                        ELSE approval_status
                                                    END
                                                    WHERE header_id = '{data["id_retur"]}'"""
        


        sql_update_status_approval_header = f"""UPDATE trans_approval_header hh
                                                    SET approval_status = 3
                                                    WHERE header_id = '{data["id_retur"]}'
                                                    AND NOT EXISTS (
                                                        SELECT approval_status
                                                        FROM trans_approval_detail dd
                                                        WHERE dd.header_id = '{data["id_retur"]}'
                                                        AND dd.approval_status <> 3
                                                    )"""

        
        sql_insert_mutasi = f"""INSERT INTO trans_inventory_detail_mutasi (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate,in_out,mutasi_type,id_references,tabel_reference,tanggal) 
                                SELECT 
                                    aa.produk_id,
                                    bb.company_id,
                                    bb.cabang_id,
                                    aa.qty_retur as qty,
                                    dd.harga_satuan,
                                    dd.harga_total,
                                    '{datetime.today()}' as updateindb,
                                    '{auth.AuthAction.get_data_params("username")}' as userupdate,
                                    'IN' as in_out,
                                    'RT' as mutasi_type,
                                    aa.id_header as id_references,
                                    'trans_inventory_subsidiary_retur_detail' as tabel_reference,
                                    bb.tanggal_retur as tanggal
                                FROM trans_inventory_subsidiary_retur_detail aa
                                LEFT JOIN trans_inventory_subsidiary_retur_header bb
                                ON aa.id_header = bb.id_header
                                LEFT JOIN trans_inventory_subsidiary_invoice cc
                                ON bb.id_invoice = cc.id_trans
                                LEFT JOIN trans_inventory_subsidiary_sales_order dd
                                ON cc.id_trans_sales_order = dd.id_trans
                                WHERE aa.id_header = {data["id_retur"]} """


        sql_delete_inventory_detail = f"""DELETE FROM trans_inventory_detail WHERE produk_id = {data["produk_id"]} AND company_id = {data["company_id"]} AND cabang_id = {data["cabang_id"]}"""

        sql_insert_inventory_detail = f"""INSERT INTO trans_inventory_detail (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb, userupdate) 
                                        SELECT
                                            produk_id,
                                            company_id,
                                            cabang_id,
                                            qty_in - qty_out AS qty,
                                            ROUND( ( ht_in - ht_out ) / ( qty_in - qty_out ), 0 ) AS harga_satuan,
                                            ht_in - ht_out AS harga_total,
                                            '{datetime.today()}',
                                            '{auth.AuthAction.get_data_params("username")}' 
                                        FROM
                                            (
                                            SELECT
                                                produk_id,
                                                company_id,
                                                cabang_id,
                                                SUM ( CASE WHEN in_out = 'IN' THEN qty ELSE 0 END ) qty_in,
                                                SUM ( CASE WHEN in_out = 'OUT' THEN qty ELSE 0 END ) qty_out,
                                                SUM ( CASE WHEN in_out = 'IN' THEN harga_total ELSE 0 END ) ht_in,
                                                SUM ( CASE WHEN in_out = 'OUT' THEN harga_total ELSE 0 END ) ht_out 
                                        FROM
                                            trans_inventory_detail_mutasi 
                                        WHERE
                                            produk_id = {data["produk_id"]} 
                                            AND company_id = {data["company_id"]} 
                                            AND cabang_id = {data["cabang_id"]} 
                                        GROUP BY
                                            produk_id,
                                            company_id,
                                            cabang_id 
                                            ) aa"""
        
        try:
          print(sql_update_status_approval_detail)
          print("\n\n")
          print(sql_update_status_approval_header)
          print("\n\n")
          print(sql_insert_mutasi)
          print("\n\n")
          print(sql_delete_inventory_detail)
          print("\n\n")
          print(sql_insert_inventory_detail)
          print("\n\n")


          await self.db.executeTrans([sql_update_status_approval_detail, 
                                      sql_update_status_approval_header,
                                      sql_insert_mutasi,
                                      sql_delete_inventory_detail,
                                      sql_insert_inventory_detail
                                      ])

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

@app.get("/api/f_trans/c_inventory_subsidiary_retur/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    username: str = Query(None, alias="$username"),
):
    ob_data = c_inventory_subsidiary_retur_approval()
    return await ob_data.read(orderby, limit, offset, filter, company_id, cabang_id,username)

@app.post("/api/f_trans/c_inventory_subsidiary_retur_approval/approve")
async def approve(request: Request):
    data = await request.json()
    ob_data = c_inventory_subsidiary_retur_approval()
    data = data["data_where_update"]
    return await ob_data.approve(data)