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


class c_subsidiary_inventory_delivery_order(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read_history(
        self,
        orderby,
        limit,
        offset,
        filter,
        company_id=None,
        cabang_id=None,
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

        sql = (
            f"""SELECT * FROM (
                        SELECT 
                        aa.id_trans,
						aa.updateindb,
						aa.userupdate,
							(CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                            ELSE ''
                        END) as ket_status_release,
                        aa.status_release,
						aa.tanggal_do,
						aa.file_upload,
						aa.id_trans_sales_order,
					    cc.id_produk as produk_id,
                        cc.nama_produk || '(' || ee.uom_satuan || ')' AS nama_produk,
						dd.id_kategori as kategori_id,
                        dd.kategori,
						ee.id_uom_satuan,
                        ee.uom_satuan,
						ff.id_company as company_id,
                        ff.company_name,
                        gg.id_cabang as cabang_id,
                        gg.cabang_name,
						bb.qty as qty_so,
                        bb.harga_satuan as harga_satuan_so,
                        bb.harga_total as harga_total_so,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                            ELSE ''
                        END) as ket_status_release_so,
                        aa.status_release as status_release_so,
						aa.file_upload as file_upload_so,
						hh.id_customer as customer_id,
						hh.nama_customer,
						bb.ppn_percent,
						bb.ppn_value,
						bb.pph_22_percent,
						bb.pph_22_value,
						bb.harga_total_ppn_pph,
                        hh.id_customer,
						hh.nama_customer,
						hh.alamat,
						hh.alamat,
						hh.no_ktp,
						hh.no_hp,
						hh.email,
						ii.kode_prov,
						ii.nama as nama_prov,
						jj.kode_kotakab,
						jj.nama as nama_kotakab,
						kk.kode_kec,
						kk.nama as nama_kec,
						ll.kode_kel,
						ll.nama as nama_kel,
                        cc.ppn,
                        cc.pph22
                    FROM trans_inventory_subsidiary_delivery_order aa
					LEFT JOIN trans_inventory_subsidiary_sales_order bb ON aa.id_trans_sales_order = bb.id_trans
				    LEFT JOIN master_produk cc ON bb.produk_id = cc.id_produk
					LEFT JOIN master_produk_kategori dd ON cc.kategori_produk = dd.id_kategori
					LEFT JOIN master_produk_uom_satuan ee ON cc.uom_satuan = ee.id_uom_satuan
					LEFT JOIN master_company ff ON bb.company_id = ff.id_company
					LEFT JOIN master_company_cabang gg ON bb.cabang_id = gg.id_cabang AND bb.company_id = gg.id_company
				    LEFT JOIN master_customer hh ON bb.customer_id = hh.id_customer
					LEFT JOIN master_provinsi ii ON hh.kode_prov = ii.kode_prov
					LEFT JOIN master_kotakab jj ON hh.kode_prov = jj.kode_prov AND hh.kode_kotakab = jj.kode_kotakab
					LEFT JOIN master_kecamatan kk ON hh.kode_prov = kk.kode_prov AND hh.kode_kotakab = kk.kode_kotakab AND hh.kode_kec = kk.kode_kec
					LEFT JOIN master_kelurahan ll ON hh.kode_prov = ll.kode_prov AND hh.kode_kotakab = ll.kode_kotakab AND hh.kode_kec = ll.kode_kec AND hh.kode_kel = ll.kode_kel
                ) zz """
            + str_clause
        )

        print(sql)

        sql_count = (
            f"""SELECT count(*) count FROM (
                    SELECT 
                        aa.id_trans,
						aa.updateindb,
						aa.userupdate,
							(CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                            ELSE 'draft'
                        END) as ket_status_release,
                        aa.status_release,
						aa.tanggal_do,
						aa.file_upload,
						aa.id_trans_sales_order,
					    cc.id_produk as produk_id,
                        cc.nama_produk || '(' || ee.uom_satuan || ')' AS nama_produk,
						dd.id_kategori as kategori_id,
                        dd.kategori,
						ee.id_uom_satuan,
                        ee.uom_satuan,
						ff.id_company as company_id,
                        ff.company_name,
                        gg.id_cabang as cabang_id,
                        gg.cabang_name,
						bb.qty as qty_so,
                        bb.harga_satuan as harga_satuan_so,
                        bb.harga_total as harga_total_so,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                            ELSE 'draft'
                        END) as ket_status_release_so,
                        aa.status_release as status_release_so,
						aa.file_upload as file_upload_so,
						hh.id_customer as customer_id,
						hh.nama_customer,
						bb.ppn_percent,
						bb.ppn_value,
						bb.pph_22_percent,
						bb.pph_22_value,
						bb.harga_total_ppn_pph,
                        hh.id_customer,
						hh.nama_customer,
						hh.alamat,
						hh.alamat,
						hh.no_ktp,
						hh.no_hp,
						hh.email,
						ii.kode_prov,
						ii.nama as nama_prov,
						jj.kode_kotakab,
						jj.nama as nama_kotakab,
						kk.kode_kec,
						kk.nama as nama_kec,
						ll.kode_kel,
						ll.nama as nama_kel,
                        cc.ppn,
                        cc.pph22
                    FROM trans_inventory_subsidiary_delivery_order aa
					LEFT JOIN trans_inventory_subsidiary_sales_order bb ON aa.id_trans_sales_order = bb.id_trans
				    LEFT JOIN master_produk cc ON bb.produk_id = cc.id_produk
					LEFT JOIN master_produk_kategori dd ON cc.kategori_produk = dd.id_kategori
					LEFT JOIN master_produk_uom_satuan ee ON cc.uom_satuan = ee.id_uom_satuan
					LEFT JOIN master_company ff ON bb.company_id = ff.id_company
					LEFT JOIN master_company_cabang gg ON bb.cabang_id = gg.id_cabang AND bb.company_id = gg.id_company
				    LEFT JOIN master_customer hh ON bb.customer_id = hh.id_customer
					LEFT JOIN master_provinsi ii ON hh.kode_prov = ii.kode_prov
					LEFT JOIN master_kotakab jj ON hh.kode_prov = jj.kode_prov AND hh.kode_kotakab = jj.kode_kotakab
					LEFT JOIN master_kecamatan kk ON hh.kode_prov = kk.kode_prov AND hh.kode_kotakab = kk.kode_kotakab AND hh.kode_kec = kk.kode_kec
					LEFT JOIN master_kelurahan ll ON hh.kode_prov = ll.kode_prov AND hh.kode_kotakab = ll.kode_kotakab AND hh.kode_kec = ll.kode_kec AND hh.kode_kel = ll.kode_kel
                ) zz """
            + str_clause_count
        )

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
                        FROM trans_inventory_subsidiary_delivery_order 
                        WHERE company_id = {company_id} AND DATE_PART('year', tanggal_do) = {tahun} AND DATE_PART('month', tanggal_do) = {bulan}"""
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

    async def create(self, data, files: List[UploadFile], listFilename: List[str]):

        tanggal = datetime.strptime(data["tanggal"], "%Y-%m-%d")
        # tanggal = '2025-08-04'
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode = await self.get_id_trans_kode(
            data["company_id"], data["cabang_id"], "DO", tahun, bulan
        )

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "id_trans": data_kode["id_trans"],
                "no_urut": data_kode["no_urut"],
            }
        )

        # print(data)

        if len(files) > 0:
            path_parent = params.loc["file_inventory_delivery_order"]
            file_insert_query = []

            for i, v in enumerate(files):

                filename = data_kode["id_trans"] + "_" + v.filename
                path = path_parent + "/" + filename
                print(path)

                content = await v.read()
                os.makedirs(os.path.dirname(path), exist_ok=True)
                file_ = open(path, "ab")
                file_.write(content)
                file_.close()

                file_insert_query.append(
                    f"""INSERT INTO files_upload (id_trans, file_name, files)
                VALUES('{data_kode["id_trans"]}', '{listFilename[i]}', '{filename}');"""
                )

        # print(data)

        sqlString = self.db.genStrInsertSingleObject(
            data, "trans_inventory_subsidiary_delivery_order"
        )

        try:
            await self.db.executeQuery(sqlString)

            if len(files) > 0:
                try:
                    for query in file_insert_query:
                        print(query)
                        await self.db.executeQuery(query)
                except Exception as e:
                    raise HTTPException(400, ("The error is: ", str(e)))

            return "success"
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))

    async def update(self, data, files: List[UploadFile], listFilename: List[str]):

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "status_release": False,
            }
        )

        sqlString = self.db.genUpdateObject(
            data,
            {"id_trans": data["id_trans"]},
            "trans_inventory_subsidiary_delivery_order",
        )

        if len(files) > 0:
            path_parent = params.loc["file_inventory_delivery_order"]
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

        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)

            if len(files) > 0:
                try:
                    for query in file_insert_query:
                        print(query)
                        await self.db.executeQuery(query)
                except Exception as e:
                    raise HTTPException(400, ("The error is: ", str(e)))

            return "success"
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(
            data_where, "trans_inventory_holding_delivery_order"
        )

        sqlString2 = self.db.genDeleteObject(data_where, "files_upload")

        get_files = f"""SELECT files FROM files_upload WHERE id_trans = '{data_where["id_trans"]}'"""

        files = await self.db.executeToDict(get_files)
        # print(files)

        try:
            if len(files) > 0:
                for file in files:
                    path = (
                        params.loc["file_inventory_delivery_order"]
                        + "/"
                        + file["files"]
                    )
                    print(path)
                    os.remove(path)
                await self.db.executeTrans([sqlString, sqlString2])
            else:
                await self.db.executeTrans([sqlString, sqlString2])
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message

    async def delete_file(self, data):
        tbl = "files_upload"
        delete_sql = self.db.genDeleteObject({"files": data["filename"]}, tbl)

        path = str(params.loc["file_inventory_delivery_order"]) + "/" + data["filename"]
        try:
            await self.db.executeQuery(delete_sql)

            os.remove(path)
            return "success"
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("The error is: ", str(e)))

    async def read_files(self, id_trans):
        sql = f""" SELECT file_name, files FROM files_upload where id_trans = '{id_trans}' """

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

    async def release(self, data):
        try:
            data_where_update = data["data_where_update"]

            sql_update_status_release_inv_delivery_order = f"""UPDATE trans_inventory_subsidiary_delivery_order SET status_release = 'true'
            WHERE id_trans = '{data_where_update['id_trans']}'"""
            await self.db.executeQuery(sql_update_status_release_inv_delivery_order)

            message = {"status": True, "msg": "success"}

        except Exception as e:
            message = {"status": False, "msg": "Eror. Cek query."}
            raise HTTPException(400, "The error is: " + str(e))
        return message


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_subsidiary_inventory_delivery_order/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_subsidiary_inventory_delivery_order/read_history")
async def read_history(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
):
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.read_history(
        orderby, limit, offset, filter, company_id, cabang_id
    )


@app.post("/api/f_trans/c_subsidiary_inventory_delivery_order/create")
async def create(
    tanggal_do: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {"tanggal_do": tanggal_do}
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.create(data, files, filename)


@app.post("/api/f_trans/c_subsidiary_inventory_delivery_order/update")
async def update_data(
    tanggal_do: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {"tanggal_do": tanggal_do}

    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.update(data, files, filename)


@app.get("/api/f_trans/c_subsidiary_inventory_delivery_order/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_subsidiary_inventory_delivery_order/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_subsidiary_inventory_delivery_order()
    path_parent = params.loc["file_inventory_submit"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)


@app.post("/api/f_trans/c_subsidiary_inventory_delivery_order/delete")
async def delete(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    # data = data['data']
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.delete(data)


@app.post("/api/f_trans/c_subsidiary_inventory_delivery_order/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.delete_file(data)


@app.post("/api/f_trans/c_subsidiary_inventory_delivery_order/release")
async def release(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.release(data)


@app.get("/api/f_trans/c_subsidiary_inventory_delivery_order/get_id_trans_kode")
async def get_id_trans_kode(company_id, cabang_id, kode_trans, tahun, bulan):
    ob_data = c_subsidiary_inventory_delivery_order()
    return await ob_data.get_id_trans_kode(
        company_id, cabang_id, kode_trans, tahun, bulan
    )
