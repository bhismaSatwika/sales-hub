from datetime import datetime
import io
import json
import mimetypes
from typing import List, Optional
from fastapi import HTTPException, Query, Request, Form, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from config import params
from library.router import app
from library.db import Db
from library import *
import os
from modules import f_master
from modules import f_trans
import asyncio
from modules.f_trans.sales_order_create_pdf import PDF
from modules.f_trans.delivery_order_create_pdf import PDF as PDF_DO
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font

class c_trans_inventory_holding_delivery_preparation(object):
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
                    trans_inventory_holding_delivery_preparation_header aa
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
                    trans_inventory_holding_delivery_preparation_header aa
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

        # print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data
    
    async def update(
        self, data, files: List[UploadFile], listFilename: List[str], product: List[str]
    ):
        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today()
            }
        )

        sqlHeader = self.db.genUpdateObject(
            data,
            {"id_trans": data["id_trans"]},
            "trans_inventory_holding_delivery_preparation_header",
        )

        if len(files) > 0:
            path_parent = params.loc["file_inventory_sales_order"]
            file_insert_query = []

            for i, v in enumerate(files):

                filename = data["id_trans"] + "_" + v.filename
                path = path_parent + "/" + filename
                print(path)

                content = await v.read()
                os.makedirs(os.path.dirname(path), exist_ok=True)
                file_ = open(path, "ab")
                file_.write(content)
                file_.close()

                file_insert_query.append(
                    f"""INSERT INTO files_upload (id_trans, file_name, files)
                VALUES('{data["id_trans"]}', '{listFilename[i]}', '{filename}');"""
                )

        products = []

        queries = []

        try:

            if len(product) != 0:

                products = [
                    {
                        **json.loads(p),
                        "id_trans": data["id_trans"],
                        "company_id": data["company_id"],
                        "cabang_id": data["cabang_id"],
                        "userupdate": auth.AuthAction.get_data_params("username"),
                    }
                    for p in product
                ]

                sqlProduct = self.db.genStrInsertArrayObject(
                    products, "trans_inventory_holding_delivery_preparation"
                )
                print(sqlProduct)

                queries.append(str(sqlProduct))

            queries.append(str(sqlHeader))

            trans = await self.db.executeTrans(queries)

            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", str(e)))

            if len(files) > 0:
                try:
                    for query in file_insert_query:
                        print(query)
                        await self.db.executeQuery(query)
                except Exception as e:
                    print(e)
                    raise HTTPException(400, ("The error is: ", str(e)))

            return "success"
        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_trans_inventory_holding_delivery_preparation/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_trans_inventory_holding_delivery_preparation/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
):
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.read(orderby, limit, offset, filter, company_id, cabang_id)


@app.post("/api/f_trans/c_trans_inventory_holding_delivery_preparation/update")
async def update(
    id_trans: str = Form(...),
    company_id: int = Form(...),
    cabang_id: int = Form(...),
    salesman: int = Form(...),
    tanggal: str = Form(...),
    customer_id: int = Form(...),
    id_pembayaran: int = Form(...),
    total_ppn: float = Form(...),
    total_pph: float = Form(...),
    harga_total_hpp: float = Form(...),
    biaya_admin: float = Form(...),
    harga_total_ppn_pph: float = Form(...),
    harga_total: float = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
    product: Optional[List[str]] = Form(default=[]),
    transport_cost: float = Form(...),
):
    data = {
        "id_trans": id_trans,
        "company_id": company_id,
        "cabang_id": cabang_id,
        "salesman": salesman,
        "tanggal": tanggal,
        "customer_id": customer_id,
        "id_pembayaran": id_pembayaran,
        "total_ppn": total_ppn,
        "total_pph": total_pph,
        "harga_total_hpp": harga_total_hpp,
        "biaya_admin": biaya_admin,
        "harga_total_ppn_pph": harga_total_ppn_pph,
        "harga_total": harga_total,
        "transport_cost": transport_cost,
    }
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.update(data, files, filename, product)