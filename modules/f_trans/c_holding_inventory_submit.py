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


class c_holding_inventory_submit(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):
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
                        bb.ppn,
                        bb.pph22
                    FROM trans_inventory_holding_submit aa
                    LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                    LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                    LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang and aa.company_id = ff.id_company
                ) zz """
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) count FROM (
                     SELECT 
                        aa.*
                    FROM trans_inventory_holding_submit aa
                    LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                    LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                    LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang and aa.company_id = ff.id_company
                ) zz """
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def get_id_trans_kode(self, company_id,cabang_id,kode_trans, tahun, bulan):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        )
        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM trans_inventory_holding_submit 
                        WHERE company_id = {company_id} AND DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
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
        # print(id_trans)

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

        data_kode = await self.get_id_trans_kode(data["company_id"],data["cabang_id"],"IS", tahun, bulan)

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "status_release": False,
                "id_trans": data_kode["id_trans"],
                "no_urut": data_kode["no_urut"],
            }
        )

        if len(files) > 0:
            path_parent = params.loc["file_inventory_submit"]
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

        sqlString = self.db.genStrInsertSingleObject(data, "trans_inventory_holding_submit")

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
            data, {"id_trans": data["id_trans"]}, "trans_inventory_holding_submit"
        )

        if len(files) > 0:
            path_parent = params.loc["file_inventory_submit"]
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
        sqlString = self.db.genDeleteObject(data_where, "trans_inventory_holding_submit")

        sqlString2 = self.db.genDeleteObject(data_where, "files_upload")

        get_files = f"""SELECT files FROM files_upload WHERE id_trans = '{data_where["id_trans"]}'"""

        files = await self.db.executeToDict(get_files)
        # print(files)

        try:
            if len(files) > 0:
                for file in files:
                    path = (
                        params.loc["file_inventory_submit"] + "/" + file["files"]
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

        path = str(params.loc["file_inventory_submit"]) + "/" + data["filename"]
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
        data_where_delete = data["data_where_delete"]
        
        try:

            sql_inv_submit = f"""SELECT 
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
                                    file_upload,
                                    no_urut
                                FROM trans_inventory_holding_submit WHERE id_trans = '{data_where_update['id_trans']}'"""

            result_inv_submit = {
                "inv_submit": await self.db.executeToDict(sql_inv_submit)
            }

            data_inv_mutasi = {
                "produk_id": result_inv_submit["inv_submit"][0]["produk_id"],
                "company_id": result_inv_submit["inv_submit"][0]["company_id"],
                "cabang_id": result_inv_submit["inv_submit"][0]["cabang_id"],
                "qty": int(result_inv_submit["inv_submit"][0]["qty"]),
                "harga_satuan": int(result_inv_submit["inv_submit"][0]["harga_satuan"]),
                "harga_total": int(result_inv_submit["inv_submit"][0]["harga_total"]),
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
                "in_out": "IN",
                "mutasi_type": "ST",
                "id_references": result_inv_submit["inv_submit"][0]["id_trans"],
                "tabel_reference": "trans_inventory_holding_submit",
                "tanggal": result_inv_submit["inv_submit"][0]["tanggal"],
            }

            sql_insert_inv_mutasi = self.db.genStrInsertSingleObject(data_inv_mutasi, "trans_inventory_detail_mutasi")
            await self.db.executeQuery(sql_insert_inv_mutasi)

            
        except Exception as e:
            message = {"status": False, "msg": "Erorr saat insert mutasi"}
            print(str(e))
            raise HTTPException(status_code=400, detail=str(e))

        try:
        
            sql_update_status_release_inv_submit = f"""UPDATE trans_inventory_holding_submit SET status_release = 'true'
            WHERE id_trans = '{data_where_update['id_trans']}'"""
            # await self.db.executeQuery(sql_update_status_release_inv_submit)

        
            sql_delete_inv_detail = f"""DELETE FROM trans_inventory_detail 
                                    WHERE company_id = {data_where_delete['company_id']} AND cabang_id = {data_where_delete['cabang_id']} AND produk_id = {data_where_delete['produk_id']}"""
            # await self.db.executeQuery(sql_delete_inv_detail)

        
            sql_detail_mutasi = f"""SELECT produk_id,
                                        company_id,
                                        cabang_id,
                                        qty,
                                        ROUND(harga_total/qty,0) as harga_satuan,
                                        harga_total 
                                    FROM (
                                    SELECT
                                        produk_id,
                                        company_id,
                                        cabang_id,
                                        SUM(case when in_out ='IN' then qty else 0 end)-SUM(case when in_out ='OUT' then qty else 0 end) qty,
                                        SUM(case when in_out ='IN' then harga_total else 0 end)-SUM(case when in_out ='OUT' then harga_total else 0 end) harga_total
                                    FROM
                                        trans_inventory_detail_mutasi 
                                    WHERE company_id = {data_where_delete['company_id']} and cabang_id = {data_where_delete['cabang_id']} and produk_id = {data_where_delete['produk_id']} 
                                        GROUP BY
                                        produk_id,
                                        company_id,
                                        cabang_id
                                        ) aa"""

            data_detail_mutasi = {
                "detail_mutasi": await self.db.executeToDict(sql_detail_mutasi)
            }

            
            data_inv_detail = {
                "produk_id": data_detail_mutasi["detail_mutasi"][0]["produk_id"],
                "company_id": data_detail_mutasi["detail_mutasi"][0]["company_id"],
                "cabang_id": data_detail_mutasi["detail_mutasi"][0]["cabang_id"],
                "qty": int(data_detail_mutasi["detail_mutasi"][0]["qty"]),
                "harga_satuan": int(data_detail_mutasi["detail_mutasi"][0]["harga_satuan"]),
                "harga_total": int(data_detail_mutasi["detail_mutasi"][0]["harga_total"]),
            }

            sql_insert_inv_detail = self.db.genStrInsertSingleObject(data_inv_detail, "trans_inventory_detail")
            # await self.db.executeQuery(sql_insert_inv_detail)


            # eksekusi all transaksi insert, update, delete    
            trans = await self.db.executeTrans([
                sql_update_status_release_inv_submit,
                sql_delete_inv_detail,
                sql_insert_inv_detail
            ])

            if trans["status"] == False:
                message = {"status": False, "msg": "Eror. Cek query."}
                raise HTTPException(400, str(trans["detail"]))
                

            message = {"status": True, "msg": "success"}

        except Exception as e1:

            where_del_mutasi = {
                'id_trans' : data_detail_mutasi["detail_mutasi"][0]["id_trans"],
            }

            sql_del_mutasi = await self.db.genDeleteObject(where_del_mutasi,'trans_inventory_detail_mutasi')

            try:
                await self.db.executeTrans([sql_del_mutasi])
            except Exception as e2:
                print(str(e2))
                raise HTTPException(400, ("error ketika delete di mutasi: ", str(e2)))

            
            message = {"status": False, "msg": "Eror. Cek query."}
            print(str(e1))
            raise HTTPException(status_code=400, detail=str(e1))

        
        return message
    

    async def release_test(self):
        try:
            sql_update_status_release_inv_submit = f"""UPDATE trans_inventory_holding_submit SET status_release = 'true'
            WHERE id_trans = 'HLD.IS.01.2025.08.0001'"""
            # print(sql_update_status_release_inv_submit)

            sql_inv_submit = f"""SELECT 
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
                                    file_upload,
                                    no_urut
                                FROM trans_inventory_holding_submit WHERE id_trans = 'HLD.IS.01.2025.08.0001'"""

            # print(sql_inv_submit)

            result_inv_submit = {
                "inv_submit": await self.db.executeToDict(sql_inv_submit)
            }

            # print(result_inv_submit['inv_submit'][0]['harga_total'])

            data_inv_mutasi = {
                "produk_id": result_inv_submit["inv_submit"][0]["produk_id"],
                "company_id": result_inv_submit["inv_submit"][0]["company_id"],
                "cabang_id": result_inv_submit["inv_submit"][0]["cabang_id"],
                "qty": result_inv_submit["inv_submit"][0]["qty"],
                "harga_satuan": result_inv_submit["inv_submit"][0]["harga_satuan"],
                "harga_total": result_inv_submit["inv_submit"][0]["harga_total"],
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
                "in_out": "IN",
                "mutasi_type": "ST",
                "id_references": result_inv_submit["inv_submit"][0]["id_trans"],
                "tabel_reference": "trans_inventory_holding_submit",
                "tanggal": result_inv_submit["inv_submit"][0]["tanggal"],
            }

            # print(data_inv_mutasi)

            sql_insert_inv_mutasi = self.db.genStrInsertSingleObject(
                data_inv_mutasi, "trans_inventory_detail_mutasi"
            )
            # print(sql_insert_inv_mutasi)

            sql_delete_inv_detail = f"""DELETE FROM trans_inventory_detail 
                                    WHERE company_id = 1 AND cabang_id = 1 AND produk_id = 1"""
            # print(sql_delete_inv_detail)

            sql_detail_mutasi = f"""SELECT
                                        produk_id,
                                        company_id,
                                        cabang_id,
                                        qty,
                                        CASE 
                                            WHEN qty <> 0 
                                            THEN ROUND(harga_total / qty, 0)
                                            ELSE 0
                                        END AS harga_satuan,
                                        harga_total
                                    FROM (
                                        SELECT
                                            produk_id,
                                            company_id,
                                            cabang_id,
                                            SUM(CASE WHEN in_out = 'in' THEN qty ELSE 0 END) - SUM(CASE WHEN in_out = 'out' THEN qty ELSE 0 END) AS qty,
                                            SUM(CASE WHEN in_out = 'in' THEN harga_total ELSE 0 END) - SUM(CASE WHEN in_out = 'out' THEN harga_total ELSE 0 END) AS harga_total
                                        FROM trans_inventory_detail_mutasi 
                                        WHERE company_id = 1 
                                        AND cabang_id = 1 
                                        AND produk_id = 1 
                                        GROUP BY produk_id, company_id, cabang_id
                                    ) aa;
                                    """
            # print(sql_detail_mutasi)

            result_detail_mutasi = {
                "detail_mutasi": await self.db.executeToDict(sql_detail_mutasi)
            }

            data_inv_detail = {
                "produk_id": result_detail_mutasi["detail_mutasi"][0]["produk_id"],
                "company_id": result_detail_mutasi["detail_mutasi"][0]["company_id"],
                "cabang_id": result_detail_mutasi["detail_mutasi"][0]["cabang_id"],
                "qty": result_detail_mutasi["detail_mutasi"][0]["qty"],
                "harga_satuan": result_detail_mutasi["detail_mutasi"][0]["harga_satuan"],
                "harga_total": result_detail_mutasi["detail_mutasi"][0]["harga_total"],
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
            }

            sql_insert_inv_detail = self.db.genStrInsertSingleObject(
                data_inv_detail, "trans_inventory_detail"
            )
            print(sql_insert_inv_detail)

            message = {"status": True, "msg": "success"}
        except Exception as e:
            message = {"status": False, "msg": "Eror. Cek query."}
            raise HTTPException(status_code=400, detail=str(e))

        return message


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_holding_inventory_submit/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_holding_inventory_submit/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    ob_data = c_holding_inventory_submit()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_trans/c_holding_inventory_submit/create")
async def create(
    cabang_id: int = Form(...),
    company_id: int = Form(...),
    harga_satuan: float = Form(...),
    harga_total: float = Form(...),
    produk_id: int = Form(...),
    qty: int = Form(...),
    tanggal: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
):
    data = {
        "cabang_id": cabang_id,
        "company_id": company_id,
        "harga_satuan": harga_satuan,
        "harga_total": harga_total,
        "produk_id": produk_id,
        "qty": qty,
        "tanggal": tanggal,
    }
    ob_data = c_holding_inventory_submit()
    return await ob_data.create(data, files, filename)


@app.post("/api/f_trans/c_holding_inventory_submit/update")
async def update(
    id_trans: str = Form(...),
    cabang_id: int = Form(...),
    company_id: int = Form(...),
    harga_satuan: float = Form(...),
    harga_total: float = Form(...),
    produk_id: int = Form(...),
    qty: int = Form(...),
    tanggal: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = File([]),
):
    data = {
        "id_trans": id_trans,
        "cabang_id": cabang_id,
        "company_id": company_id,
        "harga_satuan": harga_satuan,
        "harga_total": harga_total,
        "produk_id": produk_id,
        "qty": qty,
        "tanggal": tanggal,
    }
    ob_data = c_holding_inventory_submit()
    return await ob_data.update(data, files, filename)


@app.get("/api/f_trans/c_holding_inventory_submit/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_holding_inventory_submit()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_holding_inventory_submit/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_holding_inventory_submit()
    path_parent = params.loc["file_inventory_submit"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)


@app.post("/api/f_trans/c_holding_inventory_submit/delete")
async def delete(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    # data = data['data']
    ob_data = c_holding_inventory_submit()
    return await ob_data.delete(data)


@app.post("/api/f_trans/c_holding_inventory_submit/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_holding_inventory_submit()
    return await ob_data.delete_file(data)


@app.post("/api/f_trans/c_holding_inventory_submit/release")
async def release(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    ob_data = c_holding_inventory_submit()
    return await ob_data.release(data)


@app.get("/api/f_trans/c_holding_inventory_submit/get_id_trans_kode")
async def get_id_trans_kode(company_id,cabang_id, kode_trans, tahun, bulan):
    ob_data = c_holding_inventory_submit()
    return await ob_data.get_id_trans_kode(company_id,cabang_id,kode_trans, tahun, bulan)


@app.post("/api/f_trans/c_holding_inventory_submit/release_test")
async def release_test():
    # data = await request.json()
    # data = json.loads(data['param'])
    ob_data = c_holding_inventory_submit()
    return await ob_data.release_test()
