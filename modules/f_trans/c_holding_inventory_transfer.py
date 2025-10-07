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


class c_holding_inventory_transfer(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == '':
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
                    (CASE 
                        WHEN aa.status_release = true
                        THEN 'release'
                    ELSE 'draft'
                    END) as ket_status_release,
                    aa.status_release,
                    aa.tanggal,
                    aa.file_upload,
                    aa.no_urut,
                    aa.updateindb,
                    aa.to_company_id,
                    aa.to_cabang_id,
                    gg.company_name as to_company,
					hh.cabang_name as to_cabang,
                    aa.transport_cost_total,
                    bb.ppn,
                    bb.pph22
                FROM trans_inventory_holding_transfer aa
                LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang AND aa.company_id = ff.id_company
                LEFT JOIN master_company gg ON aa.to_company_id = gg.id_company
				LEFT JOIN master_company_cabang hh ON aa.to_cabang_id = hh.id_cabang AND aa.to_company_id = hh.id_company
                ) zz """
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) count FROM (
                     SELECT 
                        aa.*
                    FROM trans_inventory_holding_transfer aa
                    LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                    LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                    LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang AND aa.company_id = ff.id_company
                    LEFT JOIN master_company gg ON aa.to_company_id = gg.id_company
					LEFT JOIN master_company_cabang hh ON aa.to_cabang_id = hh.id_cabang AND aa.company_id = hh.id_company
                ) zz """
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def get_id_trans_kode(self, company_id, cabang_id, kode_trans, tahun: int, bulan: int):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        )
        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM trans_inventory_holding_transfer 
                        WHERE company_id = {company_id} AND cabang_id = {cabang_id} AND DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        id_trans = (
            str(kode_company[0]["kode"])
            + "."
            +str(cabang_id)
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
    

    async def get_id_trans_kode_release(self,to_company_id,to_cabang_id,kode_trans, tahun: int, bulan: int):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {to_company_id}"""
        )
        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM trans_inventory_subsidiary_receipt_transfer 
                        WHERE company_id = {to_company_id} AND cabang_id = {to_cabang_id} AND DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        id_trans = (
            str(kode_company[0]["kode"])+ "."+str(to_cabang_id)+"."+kode_trans+ "."+ str(tahun)+ "."+ str(str(bulan).zfill(2) + "." + no_urut[0]["current_no_urut_convert"])
        )

        data_kode = {
            "id_trans": id_trans,
            "no_urut": no_urut[0]["current_no_urut_convert"],
        }

        return data_kode



    async def create(self, data, files: List[UploadFile], listFilename: List[str]):

        tanggal = datetime.strptime(data["tanggal"], "%Y-%m-%d")
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode = await self.get_id_trans_kode(data["company_id"], data["cabang_id"], "TR", tahun, bulan)

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "status_release": False,
                "id_trans": data_kode["id_trans"],
                "no_urut": data_kode["no_urut"],
            }
        )

        # print(data)

        if len(files) > 0:
            path_parent = params.loc["file_inventory_transfer"]
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
            data, "trans_inventory_holding_transfer"
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
            data, {"id_trans": data["id_trans"]}, "trans_inventory_holding_transfer"
        )

        if len(files) > 0:
            path_parent = params.loc["file_inventory_transfer"]
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
            data_where, "trans_inventory_holding_transfer"
        )

        sqlString2 = self.db.genDeleteObject(data_where, "files_upload")

        get_files = f"""SELECT files FROM files_upload WHERE id_trans = '{data_where["id_trans"]}'"""

        files = await self.db.executeToDict(get_files)
        # print(files)

        try:
            if len(files) > 0:
                for file in files:
                    path = params.loc["file_inventory_transfer"] + "/" + file["files"]
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

        path = str(params.loc["file_inventory_transfer"]) + "/" + data["filename"]
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
        data_where_update = data["data_where_update"]

        try:
          
            #-------------------------------------- untuk kebutuhan validasi ---------------------------------
            sql_valid_detail = f"""SELECT count(*) as count
                            FROM trans_inventory_detail"""
            
            result_valid_detail = {
                "inv_detail": await self.db.executeToDict(sql_valid_detail)
            }

            sql_valid_inventory = f"""SELECT 
                                        COALESCE(aa.qty_inventori,0) as qty_inventori,
                                        COALESCE(bb.qty_transfer,0) as qty_transfer,
                                        COALESCE(aa.qty_inventori,0) - COALESCE(bb.qty_transfer,0) as qty_validasi
                                    FROM (
                                    SELECT
                                        produk_id,
	                                    company_id,
	                                    cabang_id,
                                        SUM(qty) as qty_inventori
                                    FROM trans_inventory_detail 
                                    WHERE produk_id = {data_where_update['produk_id']} AND company_id = {data_where_update['company_id']} AND cabang_id = {data_where_update['cabang_id']}
                                    GROUP BY produk_id,produk_id,company_id,cabang_id
                                    ) aa
                                    LEFT JOIN (
                                        SELECT 
                                            xx.produk_id,
                                            xx.status_release,
                                            SUM(xx.qty) as qty_transfer 
                                        FROM trans_inventory_holding_transfer xx
                                    LEFT JOIN (SELECT * FROM trans_inventory_subsidiary_receipt_transfer 
                                    WHERE status_release = false AND company_id = {data_where_update['company_id']} AND cabang_id = {data_where_update['cabang_id']}) yy 
                                    ON xx.id_trans = yy.id_trans_holding_transfer
                                    
                                    WHERE xx.produk_id = {data_where_update['produk_id']} AND xx.status_release = true AND yy.id_trans_holding_transfer IS NOT NULL
                                    GROUP BY xx.produk_id,xx.status_release
                                    
                                    ) bb
                                    ON aa.produk_id = bb.produk_id"""
            
            result_inv_valid = {
                "inv_valid": await self.db.executeToDict(sql_valid_inventory)
            }
            
            sql_valid_transfer = f"""select qty from trans_inventory_holding_transfer where id_trans = '{data_where_update['id_trans']}'"""
            result_valid_transfer = {
                "inv_transfer": await self.db.executeToDict(sql_valid_transfer)
            }
            
            # if True :
            if (result_valid_detail["inv_detail"][0]["count"] != 0 
                and result_valid_detail["inv_detail"][0]["count"] != None 
                and result_valid_transfer["inv_transfer"][0]["qty"] <= result_inv_valid["inv_valid"][0]["qty_validasi"]):

                sql_update_status_release_inv_transfer = f"""UPDATE trans_inventory_holding_transfer SET status_release = 'true'
                WHERE id_trans = '{data_where_update['id_trans']}'"""
                # await self.db.executeQuery(sql_update_status_release_inv_transfer)

                sql_inv_transfer = f"""SELECT 
                                        id_trans,
                                        produk_id,
                                        company_id,
                                        cabang_id,
                                        qty,
                                        harga_satuan,
                                        harga_total,
                                        updateindb,
                                        userupdate,
                                        status_release,
                                        tanggal,
                                        to_company_id,
                                        to_cabang_id,
                                        transport_cost_total
                                    FROM trans_inventory_holding_transfer WHERE id_trans = '{data_where_update['id_trans']}'"""

                result_inv_transfer = {
                    "inv_transfer": await self.db.executeToDict(sql_inv_transfer)
                }

                tanggal = result_inv_transfer["inv_transfer"][0]["tanggal"]
                tahun = tanggal.year
                bulan = tanggal.month

                data_kode = await self.get_id_trans_kode_release(
                    result_inv_transfer["inv_transfer"][0]["to_company_id"],
                    result_inv_transfer["inv_transfer"][0]["to_cabang_id"],
                    "TR", 
                    tahun, 
                    bulan)

                data_inv_receipt_transfer = {
                    "id_trans": data_kode["id_trans"],
                    "company_id": result_inv_transfer["inv_transfer"][0]["to_company_id"],
                    "cabang_id": result_inv_transfer["inv_transfer"][0]["to_cabang_id"],
                    "updateindb": datetime.today(),
                    "userupdate": auth.AuthAction.get_data_params("username"),
                    "status_release": False,
                    "tanggal": result_inv_transfer["inv_transfer"][0]["tanggal"],
                    "id_trans_holding_transfer": result_inv_transfer["inv_transfer"][0]["id_trans"],
                    "no_urut": data_kode["no_urut"],
                }

                sql_insert_inv_receipt_transfer = self.db.genStrInsertSingleObject(
                    data_inv_receipt_transfer, "trans_inventory_subsidiary_receipt_transfer"
                )
                # await self.db.executeQuery(sql_insert_inv_receipt_transfer)

                # eksekusi all transaksi insert, update, delete    
                trans = await self.db.executeTrans([
                    sql_update_status_release_inv_transfer,
                    sql_insert_inv_receipt_transfer
                ])

                if trans["status"] == False:
                    message = {"status": False, "msg": "Eror. Cek query."}
                    raise HTTPException(400, str(trans["detail"]))
                    
                message = {"status": True, "msg": "success"}
        
            else :
                
                message = "celJumlah stok/kuantiti untuk produk "+str(result_valid_detail["inv_detail"][0]["count"])
                
                raise HTTPException(message)    

        except Exception as e:
            message = {"status": False, "msg": "Eror. Cek query."}
            print(str(e))
            raise HTTPException(status_code=400, detail=str(e))

        return message
    

    async def get_harga_satuan_inv_detail(self, id_company, id_cabang, id_produk):
        sql = f"""SELECT harga_satuan FROM trans_inventory_detail  WHERE company_id = {id_company} AND cabang_id = {id_cabang} AND produk_id = {id_produk}"""
        try:
            result = await self.db.executeToDict(sql)
            # print(result)
            if len(result) == 0:
                return 0
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
            
        return result[0]["harga_satuan"]


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_holding_inventory_transfer/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_holding_inventory_transfer/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    ob_data = c_holding_inventory_transfer()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_trans/c_holding_inventory_transfer/create")
async def create(
    produk_id: int = Form(...),
    cabang_id: int = Form(...),
    company_id: int = Form(...),
    qty: int = Form(...),
    harga_satuan: float = Form(...),
    harga_total: float = Form(...),
    tanggal: str = Form(...),
    to_company_id: int = Form(...),
    to_cabang_id: int = Form(...),
    transport_cost_total: float = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {
        "produk_id": produk_id,
        "cabang_id": cabang_id,
        "company_id": company_id,
        "qty": qty,
        "harga_satuan": harga_satuan,
        "harga_total": harga_total,
        "tanggal": tanggal,
        "to_company_id": to_company_id,
        "to_cabang_id": to_cabang_id,
        "transport_cost_total": transport_cost_total,
    }
    ob_data = c_holding_inventory_transfer()
    return await ob_data.create(data, files, filename)


@app.post("/api/f_trans/c_holding_inventory_transfer/update")
async def update(
    id_trans: str = Form(...),
    produk_id: int = Form(...),
    cabang_id: int = Form(...),
    company_id: int = Form(...),
    qty: int = Form(...),
    harga_satuan: float = Form(...),
    harga_total: float = Form(...),
    tanggal: str = Form(...),
    to_company_id: int = Form(...),
    to_cabang_id: int = Form(...),
    transport_cost_total: float = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {
        "id_trans": id_trans,
        "produk_id": produk_id,
        "cabang_id": cabang_id,
        "company_id": company_id,
        "qty": qty,
        "harga_satuan": harga_satuan,
        "harga_total": harga_total,
        "tanggal": tanggal,
        "to_company_id": to_company_id,
        "to_cabang_id": to_cabang_id,
        "transport_cost_total": transport_cost_total,
    }
    ob_data = c_holding_inventory_transfer()
    return await ob_data.update(data, files, filename)


@app.get("/api/f_trans/c_holding_inventory_transfer/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_holding_inventory_transfer()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_holding_inventory_transfer/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_holding_inventory_transfer()
    path_parent = params.loc["file_inventory_submit"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)


@app.post("/api/f_trans/c_holding_inventory_transfer/delete")
async def delete(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    # data = data['data']
    ob_data = c_holding_inventory_transfer()
    return await ob_data.delete(data)


@app.post("/api/f_trans/c_holding_inventory_transfer/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_holding_inventory_transfer()
    return await ob_data.delete_file(data)


@app.post("/api/f_trans/c_holding_inventory_transfer/release")
async def release(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    ob_data = c_holding_inventory_transfer()
    return await ob_data.release(data)


@app.get("/api/f_trans/c_holding_inventory_transfer/get_id_trans_kode")
async def get_id_trans_kode(company_id, cabang_id,kode_trans, tahun, bulan):
    ob_data = c_holding_inventory_transfer()
    return await ob_data.get_id_trans_kode(company_id, cabang_id, kode_trans, tahun, bulan)

@app.get("/api/f_trans/c_holding_inventory_transfer/get_harga_satuan_inv_detail")
async def get_harga_satuan_inv_detail(id_company, id_cabang, id_produk):
    ob_data = c_holding_inventory_transfer()
    return await ob_data.get_harga_satuan_inv_detail(id_company, id_cabang, id_produk)


