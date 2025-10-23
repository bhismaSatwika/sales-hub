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



class c_inventory_subsidiary_retur(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self,orderby,limit,offset,filter,company_id=None,cabang_id=None,filter_other="",filter_other_conj=""):
        if company_id != None and cabang_id != None:
            filter_other = (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
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
                    aa.id_header,
                    aa.id_invoice,
                    aa.tanggal_retur,
                    aa.status_release,
                    aa.updateindb,
                    aa.userupdate,
                    bb.id_trans_sales_order,
                    bb.id_trans_delivery_order,
                    bb.status_invoice,
                    bb.produk_id,
                    bb.tanggal_invoice,
                    bb.status_invoice,
                    bb.qty as qty_invoice,
                    bb.customer_id,
                    cc.nama_customer,
                    cc.company_id,
                    cc.cabang_id,
                    dd.company_name,
                    ee.cabang_name
                FROM trans_inventory_subsidiary_retur_header aa
                LEFT JOIN trans_inventory_subsidiary_invoice bb ON aa.id_invoice = bb.id_trans
                LEFT JOIN master_customer cc ON aa.customer_id = cc.id_customer
                LEFT JOIN master_company dd ON cc.company_id = dd.id_company
                LEFT JOIN master_company_cabang ee ON cc.cabang_id = ee.id_cabang) zz """+str_clause

        sql_count = f"""SELECT (*) as count FROM (
                SELECT 
                    aa.id_header,
                    aa.id_invoice,
                    aa.tanggal_retur,
                    aa.status_release,
                    aa.updateindb,
                    aa.userupdate,
                    bb.id_trans_sales_order,
                    bb.id_trans_delivery_order,
                    bb.status_invoice,
                    bb.produk_id,
                    bb.tanggal_invoice,
                    bb.status_invoice,
                    bb.qty as qty_invoice,
                    bb.customer_id,
                    cc.nama_customer,
                    cc.company_id,
                    cc.cabang_id,
                    dd.company_name,
                    ee.cabang_name  
                FROM trans_inventory_subsidiary_retur_header aa
                LEFT JOIN trans_inventory_subsidiary_invoice bb ON aa.id_invoice = bb.id_trans
                LEFT JOIN master_customer cc ON aa.customer_id = cc.id_customer
                LEFT JOIN master_company dd ON cc.company_id = dd.id_company
                LEFT JOIN master_company_cabang ee ON cc.cabang_id = ee.id_cabang) zz """+str_clause_count

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def get_id_trans_kode(self, company_id, cabang_id, kode_trans, tahun, bulan):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        )
        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM trans_inventory_subsidiary_retur_header 
                        WHERE company_id = {company_id} AND cabang_id = {cabang_id} AND DATE_PART('year', tanggal_retur) = {tahun} AND DATE_PART('month', tanggal_retur) = {bulan}"""
        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        id_trans = (
            str(kode_company[0]["kode"])
            + "."
            + str(cabang_id)
            + "."
            + kode_trans
            + "."
            + str(tahun)
            + "."
            + str(str(bulan).zfill(2) + "." + no_urut[0]["current_no_urut_convert"])
        )

        data_kode = {
            "id_trans": id_trans,
            "no_urut": no_urut[0]["current_no_urut_convert"],
        }

        return data_kode


    async def read_files(self, id_header):
        sql = f""" SELECT file_name, files FROM files_retur where id_header = '{id_header}' """

        result = await self.db.executeToDict(sql)

        output_list = [
            {"file_name": item["file_name"], "file": {"name": item["files"]}}
            for item in result
        ]
        return output_list
    

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


    async def delete_file(self, data):
        tbl = "files_upload"
        delete_sql = self.db.genDeleteObject({"files": data["filename"]}, tbl)

        path = str(params.loc["file_sales_retur"]) + "/" + data["filename"]
        try:
            await self.db.executeQuery(delete_sql)

            os.remove(path)
            return "success"
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("The error is: ", str(e)))
        

    async def create(self, data):
        tanggal = datetime.strptime(data["tanggal_retur"], "%Y-%m-%d")

        tahun = tanggal.year
        bulan = tanggal.month

        data_kode = await self.get_id_trans_kode(
            data["company_id"], data["cabang_id"], "RT", tahun, bulan
        )

        data_header = {
            "id_header" : data_kode["id_trans"],
            "id_invoice": data["id_invoice"],
            "tanggal_retur": data["tanggal_retur"],
            "status_release": False,
            "updateindb": datetime.today(),
            "userupdate": auth.AuthAction.get_data_params("username"),
            "no_urut": data_kode["no_urut"]
        }

        sql_detail = f"""INSERT INTO trans_inventory_subsidiary_retur_detail (id_header,produk_id,qty_order) 
        SELECT '{data_kode["id_trans"]}' AS id_header,produk_id,qty FROM trans_inventory_subsidiary_invoice WHERE id_trans = {data["id_invoice"]}"""

        
        sql_header = self.db.genStrInsertSingleObject(
            data_header, "trans_inventory_subsidiary_retur_header"
        )

        try:
            await self.db.executeTrans([sql_header,sql_detail])

            return "success"
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))


    async def update(self, data, files: List[UploadFile], listFilename: List[str]):
        try:
            sql_detail = self.db.genUpdateObject(
                data, {"id_detail": data["id_detail"]}, "trans_inventory_subsidiary_retur_detail"
            )

            await self.db.executeQuery(sql_detail)

            if len(files) > 0:
                path_parent = params.loc["file_sales_retur"]
                file_insert_query = []

                for i, v in enumerate(files):

                    filename = data["id_header"] + "_" + v.filename
                    path = path_parent + "/" + filename
                    print(path)

                    content = await v.read()
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    file_ = open(path, "ab")
                    file_.write(content)
                    file_.close()

                    file_insert_query.append(
                        f"""INSERT INTO files_retur (id_header, file_name, files)
                    VALUES('{data["id_header"]}', '{listFilename[i]}', '{filename}');"""
                    )


            if len(files) > 0:
                try:
                    for query in file_insert_query:
                        # print(query)
                        await self.db.executeQuery(query)
                except Exception as e:
                    raise HTTPException(400, ("The error is: ", str(e)))

            return "success"
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))
        


    async def delete(self, data_where):
        sql_header = self.db.genDeleteObject(
            data_where, "trans_inventory_subsidiary_retur_header"
        )

        sql_detail = self.db.genDeleteObject(
            data_where, "trans_inventory_subsidiary_retur_detail"
        )

        sql_file = self.db.genDeleteObject(data_where, "files_upload")

        get_files = f"""SELECT files FROM files_upload WHERE id_header = '{data_where["id_header"]}'"""

        files = await self.db.executeToDict(get_files)

        try:
            if len(files) > 0:
                for file in files:
                    path = params.loc["file_sales_retur"] + "/" + file["files"]
                    print(path)
                    os.remove(path)
                await self.db.executeTrans([sql_header, sql_detail,sql_file])
            else:
                await self.db.executeTrans([sql_header, sql_detail,sql_file])
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message



"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_inventory_subsidiary_retur/testing
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
):
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.read(orderby, limit, offset, filter, company_id, cabang_id)


@app.get("/api/f_trans/c_inventory_subsidiary_retur/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.read_files(id_trans)

@app.get("/api/f_trans/c_inventory_subsidiary_retur/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_inventory_subsidiary_retur()
    path_parent = params.loc["file_inventory_retur"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)

@app.post("/api/f_trans/c_inventory_subsidiary_retur/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.delete_file(data)

@app.get("/api/f_trans/c_inventory_subsidiary_retur/get_id_trans_kode")
async def get_id_trans_kode(company_id, cabang_id, kode_trans, tahun, bulan):
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.get_id_trans_kode(
        company_id, cabang_id, kode_trans, tahun, bulan
    )


@app.post("/api/f_trans/c_inventory_subsidiary_retur/create")
async def create(
    id_invoice: str = Form(...),
    tanggal_retur: str = Form(...),
    ):
    data = {
        "id_invoice": id_invoice,
        "tanggal_retur": tanggal_retur,
    }
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.create(data)


@app.post("/api/f_trans/c_inventory_subsidiary_retur/update")
async def update_data(
    id_invoice: str = Form(...),
    tanggal_retur: str = Form(...),
    qty_retur: int = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {
        "id_invoice": id_invoice,
        "tanggal_retur": tanggal_retur,
        "qty_retur" : qty_retur,
        }

    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.update(data, files, filename)


@app.post("/api/f_trans/c_inventory_subsidiary_retur/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_inventory_subsidiary_retur()
    return await ob_data.delete(data)
