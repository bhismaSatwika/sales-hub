import base64
from typing import Optional

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_inventory_subsidiary_invoice_canceled(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        order_type,
        company_id=None,
        cabang_id=None,
        is_pusat=False,
        filter_other="",
        filter_other_conj="",
    ):

        if company_id != None and cabang_id != None:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

            if company_id == 1:
                filter_other = f""
                filter_other_conj = f""

            elif company_id != 1 and is_pusat == True:
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

        query = f"""SELECT
            * 
            FROM
            (
                SELECT 
                A.id_trans,
                id_trans_sales_order,
                A.amount_total,
                total_product,
                A.updateindb,
                C.company_id,
                C.cabang_id,
                A.md5_file ,
                D.nama_customer
                FROM
                trans_inventory_subsidiary_invoice
                A LEFT JOIN ( SELECT id_trans, COUNT ( produk_id ) AS total_product FROM trans_inventory_subsidiary_sales_order GROUP BY id_trans ) B ON A.id_trans_sales_order = B.id_trans
                LEFT JOIN trans_inventory_subsidiary_sales_order_header C ON C.id_trans = A.id_trans_sales_order
                LEFT JOIN master_customer D on C.customer_id = D.id_customer
                WHERE c.order_type = '{order_type}' and a.is_canceled = true
            ) ZZ"""

        sql = query + str_clause
        sql_2 = sql + str_clause_count

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def get_invoice(
        self,
        orderby,
        limit,
        offset,
        filter,
        order_type,
        company_id=None,
        cabang_id=None,
        is_pusat=False,
        filter_other="",
        filter_other_conj="",
    ):

        if company_id != None and cabang_id != None:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

            if company_id == 1:
                filter_other = f""
                filter_other_conj = f""

            elif company_id != 1 and is_pusat == True:
                filter_other = f" zz.company_id = '{company_id}'"
        else:
            filter_other = f""
            filter_other_conj = f""
        if orderby == None or orderby == "":
            orderby = "zz.tanggal_invoice DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = f"""
        SELECT * FROM (
            SELECT A.id_trans,
            D.NAME AS salesman,
            A.id_trans_sales_order,
            B.harga_total,
            B.biaya_admin,
            A.tanggal_invoice,
            E.company_name,
            F.cabang_name,
            B.total_pph,
            B.total_ppn,
            B.harga_total_ppn_pph,
            G.pembayaran,
            C.nama_customer,
            b.company_id,
            b.cabang_id
            FROM
            trans_inventory_subsidiary_invoice
            A LEFT JOIN trans_inventory_subsidiary_sales_order_header B ON A.id_trans_sales_order = B.id_trans
            LEFT JOIN master_customer C ON B.customer_id = C.id_customer
            LEFT JOIN master_user D ON B.salesman = D.id_user
            LEFT JOIN master_company E ON B.company_id = E.id_company
            LEFT JOIN master_company_cabang F ON B.company_id = F.id_company 
            AND B.cabang_id = F.id_cabang
            LEFT JOIN master_jenis_pembayaran G ON B.id_pembayaran = G.id_pembayaran
            WHERE b.order_type = '{order_type}'
            ) zz
            """

        sql = query + str_clause
        sql_2 = query + str_clause_count

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data


@app.get("/api/f_trans/c_inventory_subsidiary_invoice_canceled/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    order_type: Optional[str] = Query("direct", alias="order_type"),
    is_pusat: bool = Query(None, alias="$is_pusat"),
):
    ob_data = c_inventory_subsidiary_invoice_canceled()
    return await ob_data.read(
        orderby, limit, offset, filter, order_type, company_id, cabang_id, is_pusat
    )


@app.get("/api/f_trans/c_inventory_subsidiary_invoice_canceled/get_invoice")
async def get_invoice(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    order_type: Optional[str] = Query("direct", alias="order_type"),
    is_pusat: bool = Query(None, alias="$is_pusat"),
):
    ob_data = c_inventory_subsidiary_invoice_canceled()
    return await ob_data.get_invoice(
        orderby, limit, offset, filter, order_type, company_id, cabang_id, is_pusat
    )
