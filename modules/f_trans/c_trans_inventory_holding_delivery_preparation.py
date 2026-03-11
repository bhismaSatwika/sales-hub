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
        filter_other="",
        filter_other_conj="",
        release=False,
        is_delivered=False,
    ):

        filter_other = f"""is_canceled = false and status_release = {release} and is_delivered = {is_delivered}"""

        filter_other_conj = f"and"

        if orderby == None or orderby == "":
            orderby = "a.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT
                A.*
                FROM  (
                    SELECT
                    
                    aa.id_trans,

                    aa.company_id,
                    aa.cabang_id,
                    aa.salesman,
                    aa.tanggal,
                    aa.customer_id,
                    aa.id_pembayaran,
                    aa.status_release,
                    aa.userupdate,
                    aa.updateindb,
                    ee.company_name,
                    aa.harga_total,
                    aa.transport_cost,
                    aa.grand_total,
                    ff.cabang_name,
                    ( CASE WHEN aa.status_release = TRUE THEN 'Released' ELSE 'Draft' END ) AS ket_status_release,
                    gg.nama_customer,
                    gg.account_va,
                    gg.account_bank_name,
                    hh.md5_file,
                    aa.is_delivered,
                    ii.pembayaran,
                    jj.is_canceled
                    FROM
                    trans_inventory_holding_delivery_preparation_header aa
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang 
                    AND aa.company_id = ff.id_company
                    LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer
                    LEFT JOIN trans_inventory_subsidiary_invoice hh ON aa.id_trans = hh.id_trans_sales_order
                    LEFT JOIN master_jenis_pembayaran ii ON aa.id_pembayaran = ii.id_pembayaran 
                    LEFT JOIN trans_inventory_subsidiary_invoice_pre_payment jj ON aa.id_trans_sales_order = jj.id_trans_sales_order
                ) A
             
               """
            + str_clause
        )
        print(sql)

        sql_count = (
            f"""SELECT
                COUNT(*) as count
                FROM  (
                    SELECT
                    
                    aa.id_trans,
                    
                    aa.company_id,
                    aa.cabang_id,
                    aa.salesman,
                    aa.tanggal,
                    aa.customer_id,
                    aa.id_pembayaran,
                    aa.status_release,
                    aa.userupdate,
                    aa.updateindb,
                    ee.company_name,
                    aa.harga_total,
                    ff.cabang_name,
                    ( CASE WHEN aa.status_release = TRUE THEN 'Released' ELSE 'Draft' END ) AS ket_status_release,
                    gg.nama_customer,
                    aa.is_delivered,

                    gg.account_va,
                    gg.account_bank_name,
                    hh.md5_file,
                    ii.pembayaran,
                    jj.is_canceled
                    FROM
                    trans_inventory_holding_delivery_preparation_header aa
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang 
                    AND aa.company_id = ff.id_company
                    LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer
                    LEFT JOIN trans_inventory_subsidiary_invoice hh ON aa.id_trans = hh.id_trans_sales_order
                    LEFT JOIN master_jenis_pembayaran ii ON aa.id_pembayaran = ii.id_pembayaran
                    LEFT JOIN trans_inventory_subsidiary_invoice_pre_payment jj ON aa.id_trans_sales_order = jj.id_trans_sales_order
                ) A
                """
            + str_clause_count
        )

        # print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def read_produk(
        self,
        orderby,
        limit,
        offset,
        filter,
        id_trans=None,
        filter_other="",
        filter_other_conj="",
    ):

        filter_other = f"zz.id_trans = '{id_trans}' "
        filter_other_conj = f" and "
        orderby = "zz.updateindb ASC"

        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT
                        * 
                    FROM
                        (
                        SELECT
                            aa.id_trans,
                            bb.id_produk AS produk_id,
                            bb.nama_produk || '(' || dd.uom_satuan || ')' AS nama_produk,
                            cc.id_kategori AS kategori_id,
                            cc.kategori,
                            dd.id_uom_satuan,
                            dd.uom_satuan,
                            aa.qty,
                            aa.harga_satuan,
                            aa.harga_total,
                            aa.transport_cost,
                            aa.grand_total,
                            ( CASE WHEN aa.status_release = TRUE THEN 'Release' ELSE'Draft' END ) AS ket_status_release,
                            aa.status_release,
                            aa.tanggal,
                            aa.updateindb,
                            aa.harga_satuan_hpp,
                            aa.harga_total_hpp,
                            aa.id_increment

                      
                        FROM
                            trans_inventory_holding_delivery_preparation aa
                            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                            LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                            LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                        ) zz """
            + str_clause
        )

        sql_count = (
            f"""SELECT
                        COUNT(*) as count 
                    FROM
                        (
                        SELECT
                            aa.id_trans,
                            bb.id_produk AS produk_id,
                            bb.nama_produk || '(' || dd.uom_satuan || ')' AS nama_produk,
                            cc.id_kategori AS kategori_id,
                            cc.kategori,
                            dd.id_uom_satuan,
                            dd.uom_satuan,
                            aa.qty,
                            aa.harga_satuan,
                            aa.harga_total,
                            ( CASE WHEN aa.status_release = TRUE THEN 'Release' ELSE'Draft' END ) AS ket_status_release,
                            aa.status_release,
                            aa.tanggal,
                            aa.updateindb,
                            aa.harga_satuan_hpp,
                            aa.harga_total_hpp,
                            aa.id_increment
                      
                        FROM
                            trans_inventory_holding_delivery_preparation aa
                            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                            LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                            LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                        ) zz """
            + str_clause_count
        )

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
                "updateindb": datetime.today(),
            }
        )

        sqlHeader = self.db.genUpdateObject(
            data,
            {"id_trans": data["id_trans"]},
            "trans_inventory_holding_delivery_preparation_header",
        )

        if len(files) > 0:
            path_parent = params.loc["file_delivery_preparation"]
            file_insert_query = []

            for i, v in enumerate(files):

                filename = data["id_trans"] + "_delivery" + "_" + v.filename
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

    async def update_produk(self, data):
        produk_update_where = data["product"]["update_where"]
        header_update_where = data["header"]["update_where"]

        produk_update_data = data["product"]["update_data"]
        header_update_data = data["header"]["update_data"]

        sqlProduk = self.db.genUpdateObject(
            produk_update_data,
            produk_update_where,
            "trans_inventory_holding_delivery_preparation",
        )

        sqlHeader = self.db.genUpdateObject(
            header_update_data,
            header_update_where,
            "trans_inventory_holding_delivery_preparation_header",
        )

        try:

            trans = await self.db.executeTrans([sqlProduk, sqlHeader])
            if trans["status"] == False:
                print(trans["detail"])
                raise HTTPException(400, ("The error is: ", trans["detail"]))

            return "success"

        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))

    async def read_files(self, id_trans):
        sql = f""" SELECT file_name, files FROM files_upload where id_trans = '{id_trans}' """

        result = await self.db.executeToDict(sql)

        output_list = [
            {"file_name": item["file_name"], "file": {"name": item["files"]}}
            for item in result
        ]
        return output_list

    async def release(self, data):
        data_update = {
            "userupdate": auth.AuthAction.get_data_params("username"),
            "updateindb": datetime.today(),
            "status_release": True,
            "tanggal_delivery": datetime.today(),
        }

        update_sales_order_hpp = f"""
            UPDATE trans_inventory_subsidiary_sales_order A
        SET
            harga_satuan_hpp = C.harga_satuan,
            harga_total_hpp  = C.harga_total
        FROM trans_inventory_holding_delivery_preparation_header B
        JOIN trans_inventory_holding_delivery_preparation C
            ON B.id_trans = C.id_trans
        WHERE A.id_trans = B.id_trans_sales_order AND A.produk_id = C.produk_id
        AND B.id_trans = 'NUS.12.DLV.2025.12.0001';
        
        """

        update_sales_order_header_hpp = f"""
        UPDATE trans_inventory_subsidiary_sales_order_header A
        SET
            harga_total_hpp = B.harga_total
        FROM trans_inventory_holding_delivery_preparation_header B
        WHERE A.id_trans = B.id_trans_sales_order
        AND B.id_trans = 'NUS.12.DLV.2025.12.0001';"""

        sql = self.db.genUpdateObject(
            data_update,
            data,
            "trans_inventory_holding_delivery_preparation_header",
        )

        try:
            await self.db.executeTrans(
                [sql, update_sales_order_hpp, update_sales_order_header_hpp]
            )

            return "success"

        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))

    def get_content_type(self, file_path):
        # Get the MIME type based on the file extension
        mime_type, _ = mimetypes.guess_type(file_path)
        # If the MIME type cannot be guessed, fallback to 'application/octet-stream'
        return mime_type if mime_type else "application/octet-stream"

    async def delete_file(self, data):
        tbl = "files_upload"
        delete_sql = self.db.genDeleteObject({"files": data["filename"]}, tbl)

        path = str(params.loc["file_inventory_sales_order"]) + "/" + data["filename"]
        try:
            await self.db.executeQuery(delete_sql)

            os.remove(path)
            return "success"
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("The error is: ", str(e)))

    def get_content_type(self, file_path):
        # Get the MIME type based on the file extension
        mime_type, _ = mimetypes.guess_type(file_path)
        # If the MIME type cannot be guessed, fallback to 'application/octet-stream'
        return mime_type if mime_type else "application/octet-stream"

    async def stream_file(self, path, filename):
        try:
            content_type = self.get_content_type(path)
            return FileResponse(path, media_type=content_type, filename=filename)
        except Exception as e:
            raise HTTPException(400, "The error is: " + str(e))


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
    release: Optional[bool] = Query(False, alias="release"),
    is_delivered: Optional[bool] = Query(False, alias="is_delivered"),
):
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.read(
        orderby, limit, offset, filter, company_id, cabang_id, release, is_delivered
    )


@app.get("/api/f_trans/c_trans_inventory_holding_delivery_preparation/read_produk")
async def read_produk(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    id_trans: str = Query(None, alias="id_trans"),
):
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.read_produk(orderby, limit, offset, filter, id_trans)


@app.post("/api/f_trans/c_trans_inventory_holding_delivery_preparation/update")
async def update(
    id_trans: str = Form(...),
    company_id: int = Form(...),
    cabang_id: int = Form(...),
    salesman: int = Form(...),
    tanggal: str = Form(...),
    customer_id: str = Form(...),
    id_pembayaran: int = Form(...),
    harga_total: float = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
    product: Optional[List[str]] = Form(default=[]),
    transport_cost: float = Form(...),
    grand_total: float = Form(...),
):
    data = {
        "id_trans": id_trans,
        "company_id": company_id,
        "cabang_id": cabang_id,
        "salesman": salesman,
        "tanggal": tanggal,
        "customer_id": customer_id,
        "id_pembayaran": id_pembayaran,
        "harga_total": harga_total,
        "transport_cost": transport_cost,
        "grand_total": grand_total,
    }
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.update(data, files, filename, product)


@app.post("/api/f_trans/c_trans_inventory_holding_delivery_preparation/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.release(data)


@app.post("/api/f_trans/c_trans_inventory_holding_delivery_preparation/update_produk")
async def update_produk(request: Request):
    data = await request.json()
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.update_produk(data)


@app.get("/api/f_trans/c_trans_inventory_holding_delivery_preparation/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_trans_inventory_holding_delivery_preparation/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_trans_inventory_holding_delivery_preparation()
    path_parent = params.loc["file_delivery_preparation"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)


@app.post("/api/f_trans/c_trans_inventory_holding_delivery_preparation/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_trans_inventory_holding_delivery_preparation()
    return await ob_data.delete_file(data)
