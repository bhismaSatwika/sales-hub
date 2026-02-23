import base64
from datetime import datetime
import json
import mimetypes
from typing import List, Optional

from fastapi import File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

from config import params
from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_inventory_detail_bad_stock(object):
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
        is_pusat=False,
        filter_other="",
        filter_other_conj="and",
    ):
        where = "stock_condition = 'bad' "

        if company_id != 1 and is_cabang == True:
            filter_other = where + (
                f"AND zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

        elif company_id == 1 and is_cabang == True:
            filter_other = where
            filter_other_conj = f"and"

        elif company_id == 1 and is_cabang == False:
            filter_other = where + (
                f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}'"
            )
            filter_other_conj = f" and "

        if company_id != 1 and is_pusat == True:
            filter_other = where + f" zz.company_id = '{company_id}'"
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

        query = """SELECT * FROM (
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
                bb.pph22,
                aa.stock_condition
            FROM trans_inventory_detail aa
            LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
            LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
            LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
            LEFT JOIN master_company ee ON aa.company_id = ee.id_company
            LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang AND aa.company_id = ff.id_company
            LEFT JOIN master_produk_kategori gg ON bb.kategori_produk = gg.id_kategori
            LEFT JOIN master_produk_uom_satuan hh ON bb.uom_satuan = hh.id_uom_satuan
        ) zz """

        sql = query + str_clause
        sql_2 = query + str_clause_count

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        # print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def read_request(
        self, orderby, limit, offset, filter, company_id, cabang_id, is_pusat
    ):

        filter_other = f"company_id = {company_id} AND cabang_id = {cabang_id}"
        filter_other_conj = "and"
        print(company_id, cabang_id, is_pusat)

        if company_id == 1 and cabang_id == 1:
            filter_other = f""
            filter_other_conj = f""

        if company_id != 1 and is_pusat == True:
            filter_other = f" company_id = '{company_id}'"
            filter_other_conj = f" and "

        # print(filter_other)
        if orderby == None or orderby == "":
            orderby = "A.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = """
                    SELECT A.id_trans, C.company_name, D.cabang_name, A.status_release, A.harga_total, A.approval_status, A.company_id, A.cabang_id, A.tanggal
        FROM trans_inventory_detail_bad_stock_header A
        LEFT JOIN trans_inventory_detail_bad_stock B on A.id_trans = B.id_trans
        LEFT JOIN master_company C on A.company_id = C.id_company
        LEFT JOIN master_company_cabang D on A.cabang_id = D.id_cabang
        """

        sql = query + str_clause
        sql_2 = query + str_clause_count
        print(sql)

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        # print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def read_request_produk(
        self,
        orderby,
        limit,
        offset,
        filter,
        id_trans=None,
        filter_other="",
        filter_other_conj="",
    ):

        filter_other = f"id_trans = '{id_trans}'"
        filter_other_conj = f" and "
        orderby = "A.updateindb ASC"

        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = """SELECT A.*, B.nama_produk FROM trans_inventory_detail_bad_stock A
        LEFT JOIN master_produk B on A.produk_id = B.id_produk
        """

        sql = query + str_clause
        sql_2 = query + str_clause_count

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        # print(sql)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def read_inventory_card(self, company_id, cabang_id, is_cabang):

        where = """
        """

        if company_id != 1 and is_cabang == True:
            where = f"where company_id = '{company_id}' AND cabang_id = '{cabang_id}'"

        elif company_id == 1 and is_cabang == False:
            where = f"where company_id = '{company_id}' AND cabang_id = '{cabang_id}'"

        if company_id == 2 and cabang_id == 11 and is_cabang == True:
            where = f"where company_id = '{company_id}'"

        sql_total_branch = f"""
                SELECT SUM
        ( total ) total_branch
        FROM
        ( SELECT COUNT ( DISTINCT cabang_id ) AS total FROM trans_inventory_detail {where} GROUP BY company_id ) x
        """

        sql_total_product = f""" 
        SELECT COUNT
        ( DISTINCT produk_id ) AS total_product
        FROM
        trans_inventory_detail
        {where}
        """

        sql_total_value = f"""
        SELECT SUM
        ( harga_total ) AS total_value 
        FROM
        trans_inventory_detail
        {where}
        """

        print(sql_total_branch)
        print(sql_total_product)
        print(sql_total_value)

        result_total_branch = await self.db.executeToDict(sql_total_branch)
        result_total_product = await self.db.executeToDict(sql_total_product)
        result_total_value = await self.db.executeToDict(sql_total_value)

        data = {
            "total_branch": result_total_branch[0]["total_branch"],
            "total_product": result_total_product[0]["total_product"],
            "total_value": result_total_value[0]["total_value"],
        }
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
                        FROM trans_inventory_detail_bad_stock_header
                        WHERE company_id = {company_id} AND cabang_id = {cabang_id} AND DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
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

    async def create(
        self, data, files: List[UploadFile], listFilename: List[str], product
    ):
        tanggal = datetime.strptime(data["tanggal"], "%Y-%m-%d")
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode = await self.get_id_trans_kode(
            data["company_id"], data["cabang_id"], "BS", tahun, bulan
        )

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "id_trans": data_kode["id_trans"],
                "no_urut": data_kode["no_urut"],
            }
        )

        if len(files) > 0:
            path_parent = params.loc["file_bad_stock"]
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

        sqlHeader = self.db.genStrInsertSingleObject(
            data, "trans_inventory_detail_bad_stock_header"
        )

        try:
            products = [
                {
                    **json.loads(p),
                    "id_trans": data_kode["id_trans"],
                    "userupdate": auth.AuthAction.get_data_params("username"),
                }
                for p in product
            ]

            # print(products)
            # print("\n\n")

            sqlDetail = self.db.genStrInsertArrayObject(
                products, "trans_inventory_detail_bad_stock"
            )

            # print(sqlHeader)
            # print(sqlDetail)

            trans = await self.db.executeTrans([sqlHeader, sqlDetail])

            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", str(trans["detail"])))

            if len(files) > 0:
                try:
                    for query in file_insert_query:
                        # print(query)
                        await self.db.executeQuery(query)
                except Exception as e:
                    raise HTTPException(400, ("The error is: ", str(e)))

            return "success"
        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))

    async def update_produk(self, data):
        validate = await self.validate_release(
            data["header"]["update_where"]["id_trans"]
        )
        if validate > 0:
            raise HTTPException(400, "Data sudah di release, mohon muat ulang halaman")

        produk_update_where = data["product"]["update_where"]
        header_update_where = data["header"]["update_where"]

        produk_update_data = data["product"]["update_data"]
        header_update_data = data["header"]["update_data"]

        sqlProduk = self.db.genUpdateObject(
            produk_update_data,
            produk_update_where,
            "trans_inventory_detail_bad_stock",
        )

        sqlHeader = self.db.genUpdateObject(
            header_update_data,
            header_update_where,
            "trans_inventory_detail_bad_stock_header",
        )

        # print("\n\n")
        # print(sqlProduk)
        # print("\n\n")
        # print(sqlHeader)
        # print("\n\n")

        try:

            trans = await self.db.executeTrans([sqlProduk, sqlHeader])
            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", trans["detail"]))

            return "success"

        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))

    async def update(
        self, data, files: List[UploadFile], listFilename: List[str], product: List[str]
    ):
        validate = await self.validate_release(data["id_trans"])
        if validate > 0:
            raise HTTPException(400, "Data sudah di release, mohon muat ulang halaman")
        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "status_release": False,
            }
        )

        sqlHeader = self.db.genUpdateObject(
            data,
            {"id_trans": data["id_trans"]},
            "trans_inventory_detail_bad_stock_header",
        )

        # print("daerah 1")
        # print("\n\n")
        # print(sqlDetail)

        if len(files) > 0:
            path_parent = params.loc["file_bad_stock"]
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

            # print("daerah 2")
            # print("\n\n")
            # print(products)
            # print("\n\n")

            if len(product) != 0:

                products = [
                    {
                        **json.loads(p),
                        "id_trans": data["id_trans"],
                        "userupdate": auth.AuthAction.get_data_params("username"),
                    }
                    for p in product
                ]

                # print("daerah 3")
                # print("\n\n")
                # print(products)
                # print("\n\n")

                sqlProduct = self.db.genStrInsertArrayObject(
                    products, "trans_inventory_detail_bad_stock"
                )
                print(sqlProduct)

                queries.append(str(sqlProduct))

                # print("\n\n")
                # print(queries)
                # print("\n\n")

            queries.append(str(sqlHeader))

            trans = await self.db.executeTrans(queries)

            # print("daerah 4")
            # print("\n\n")
            # print(trans)
            # print("\n\n")

            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", str(trans["detail"])))
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

    async def validate_release(self, id_trans):
        sql = f"""SELECT count(*) count FROM trans_inventory_subsidiary_sales_order_header WHERE id_trans = '{id_trans}' AND status_release = TRUE"""
        print(sql)
        res = await self.db.executeToDict(sql)
        result = res[0]["count"]
        return result

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

    async def delete_produk(self, data):
        validate = await self.validate_release(
            data["header"]["update_where"]["id_trans"]
        )
        if validate > 0:
            raise HTTPException(400, "Data sudah di release, mohon muat ulang halaman")
        produk_delete_where = data["product"]["update_where"]
        header_update_where = data["header"]["update_where"]
        header_update_data = data["header"]["update_data"]

        sqlProduk = self.db.genDeleteObject(
            produk_delete_where, "trans_inventory_detail_bad_stock"
        )

        sqlHeader = self.db.genUpdateObject(
            header_update_data,
            header_update_where,
            "trans_inventory_detail_bad_stock_header",
        )

        print("\n\n")
        print(sqlProduk)
        print("\n\n")
        print(sqlHeader)
        print("\n\n")

        try:
            trans = await self.db.executeTrans([sqlProduk, sqlHeader])

            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", str(e)))

            message = {"status": "success"}
        except Exception as e:
            print(e)
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message

    async def delete_file(self, data):
        tbl = "files_upload"
        delete_sql = self.db.genDeleteObject({"files": data["filename"]}, tbl)

        path = str(params.loc["file_bad_stock"]) + "/" + data["filename"]
        try:
            await self.db.executeQuery(delete_sql)

            os.remove(path)
            return "success"
        except Exception as e:
            print(str(e))
            raise HTTPException(400, ("The error is: ", str(e)))

    async def delete(self, data_where):
        validate = await self.validate_release(data_where["id_trans"])
        if validate > 0:
            raise HTTPException(
                400,
                ("The error is: ", "Data sudah di release, mohon muat ulang halaman"),
            )

        sqlHeader = self.db.genDeleteObject(
            data_where, "trans_inventory_detail_bad_stock_header"
        )

        sqlDetail = self.db.genDeleteObject(
            data_where, "trans_inventory_detail_bad_stock"
        )

        sqlFile = self.db.genDeleteObject(data_where, "files_upload")

        get_files = f"""SELECT files FROM files_upload WHERE id_trans = '{data_where["id_trans"]}'"""
        files = await self.db.executeToDict(get_files)

        try:
            if len(files) > 0:
                trans = await self.db.executeTrans([sqlHeader, sqlDetail, sqlFile])

                if trans["status"] == False:
                    raise HTTPException(400, ("The error is: ", str(trans["detail"])))

                for file in files:
                    path = (
                        params.loc["file_inventory_sales_order"] + "/" + file["files"]
                    )
                    print(path)
                    os.remove(path)

            else:
                await self.db.executeTrans([sqlHeader, sqlDetail, sqlFile])
            message = {"status": "success"}
        except Exception as e:
            print(e)
            message = {"status": "error"}
            raise HTTPException(status_code=400, detail=str(e))
        return message


@app.get("/api/f_trans/c_inventory_detail_bad_stock/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    is_cabang: bool = Query(None, alias="$is_cabang"),
):
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.read(
        orderby, limit, offset, filter, company_id, cabang_id, is_cabang
    )


@app.get("/api/f_trans/c_inventory_detail_bad_stock/read_request")
async def read_request(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    is_pusat: bool = Query(None, alias="is_pusat"),
):
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.read_request(
        orderby, limit, offset, filter, company_id, cabang_id, is_pusat
    )


@app.get("/api/f_trans/c_inventory_detail_bad_stock/read_produk")
async def read_produk(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    id_trans: str = Query(None, alias="id_trans"),
):
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.read_request_produk(orderby, limit, offset, filter, id_trans)


@app.get("/api/f_trans/c_inventory_detail_bad_stock/read_inventory_card")
async def read_inventory_card(
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
    is_cabang: bool = Query(None, alias="$is_cabang"),
):
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.read_inventory_card(company_id, cabang_id, is_cabang)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/create")
async def create(
    company_id: int = Form(...),
    cabang_id: int = Form(...),
    tanggal: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
    product: List[str] = Form(...),
    harga_total: float = Form(...),
):
    data = {
        "company_id": company_id,
        "cabang_id": cabang_id,
        "tanggal": tanggal,
        "harga_total": harga_total,
    }

    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.create(data, files, filename, product)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/update_produk")
async def update_produk(request: Request):
    data = await request.json()
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.update_produk(data)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.delete_file(data)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/update")
async def update(
    id_trans: str = Form(...),
    company_id: int = Form(...),
    cabang_id: int = Form(...),
    tanggal: str = Form(...),
    files: Optional[List[UploadFile]] = File([]),
    filename: Optional[List[str]] = Form(default=[]),
    product: Optional[List[str]] = Form(default=[]),
    harga_total: float = Form(...),
):
    data = {
        "id_trans": id_trans,
        "company_id": company_id,
        "cabang_id": cabang_id,
        "tanggal": tanggal,
        "harga_total": harga_total,
    }

    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.update(data, files, filename, product)


@app.get("/api/f_trans/c_inventory_detail_bad_stock/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_inventory_detail_bad_stock/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_inventory_detail_bad_stock()
    path_parent = params.loc["file_inventory_sales_order"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/delete_file")
async def delete_file(request_: Request):
    data = await request_.json()
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.delete_file(data)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/delete_produk")
async def delete_produk(request: Request):
    data = await request.json()
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.delete_produk(data)


@app.post("/api/f_trans/c_inventory_detail_bad_stock/delete")
async def delete(request: Request):
    data = await request.json()
    # data = json.loads(data['param'])
    # data = data['data']
    ob_data = c_inventory_detail_bad_stock()
    return await ob_data.delete(data)
