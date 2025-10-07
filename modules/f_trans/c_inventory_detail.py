from datetime import datetime
import json
from typing import List
from fastapi import Query, Request, Form, UploadFile, File
from library.router import app
from library.db import Db
from library import *
import os
from modules import f_master
from modules import f_trans
import asyncio


class c_trans_inventory_detail(object):
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
        is_cabang=False,
        filter_other="",
        filter_other_conj="",
    ):

        if company_id != 1 and is_cabang == True:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

        elif company_id == 1 and is_cabang == True:
            filter_other = f"(1=1)"
            filter_other_conj = f""

        elif company_id == 1 and is_cabang == False:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

        if company_id == 2 and cabang_id == 11 and is_cabang == True:
            filter_other = f" zz.company_id = '{company_id}'"
            filter_other_conj = f" and "

        # print(filter_other)
        if orderby == None or orderby == "":
            orderby = "zz.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        # print(str_clause)

        sql = (
            f"""SELECT * FROM (
            SELECT 
                aa.id_trans,
                bb.id_produk as produk_id,
                bb.nama_produk||'('||dd.uom_satuan||')' as nama_produk,
                cc.id_kategori as kategori_id,
                cc.kategori,
                dd.id_uom_satuan,
                dd.uom_satuan,
                ee.id_company as company_id,
                ee.company_name,
                ff.id_cabang as cabang_id,
                ff.cabang_name,
                aa.qty,
                aa.harga_satuan,
                aa.harga_total,
                aa.updateindb,
                bb.ppn,
                bb.pph22
            FROM trans_inventory_detail aa
            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
            LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
            LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
            LEFT JOIN master_company ee ON aa.company_id = ee.id_company
            LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang AND aa.company_id = ff.id_company
            LEFT JOIN master_produk_kategori gg ON bb.kategori_produk = gg.id_kategori
            LEFT JOIN master_produk_uom_satuan hh ON bb.uom_satuan = hh.id_uom_satuan
        ) zz """
            + str_clause
        )

        # print(sql)

        sql_count = (
            f"""SELECT count(*) count FROM (
        SELECT aa.*
            FROM trans_inventory_detail aa
            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
            LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
            LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
            LEFT JOIN master_company ee ON aa.company_id = ee.id_company
            LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang AND aa.company_id = ff.id_company
            LEFT JOIN master_produk_kategori gg ON bb.kategori_produk = gg.id_kategori
            LEFT JOIN master_produk_uom_satuan hh ON bb.uom_satuan = hh.id_uom_satuan

        ) zz """
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_trans_inventory_detail/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_trans_inventory_detail/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    is_cabang: bool = Query(None, alias="$is_cabang"),
):
    ob_data = c_trans_inventory_detail()
    return await ob_data.read(
        orderby, limit, offset, filter, company_id, cabang_id, is_cabang
    )
