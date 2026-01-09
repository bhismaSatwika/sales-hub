from datetime import datetime
import json
import mimetypes
from fastapi import HTTPException, Query, Request
from fastapi.responses import FileResponse
from config import params
from library import *
import os
from library.router import app
from library.db import Db
from modules.f_report.create_sales_report import PDF


class c_sales_order_recap(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):

        orderby = "aa.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT 
                    ROW_NUMBER() OVER (ORDER BY id DESC) AS nomor_urut,
                    aa.id,
                    aa.tanggal,
                    aa.number_report,
                    aa.status_release,
                    (CASE 
                        WHEN aa.status_release = true
                        THEN 'Release'
                    ELSE 'Draft'
                    END) as ket_status_release,
                    aa.bulan,
                    aa.tahun,
                    aa.produk_id,
                    bb.nama_produk,
                    cc.uom_satuan,
                    dd.kategori
                FROM trans_sales_recap_header aa
                LEFT JOIN master_produk bb
                ON aa.produk_id = bb.id_produk
                LEFT JOIN master_produk_uom_satuan cc
                ON bb.uom_satuan = cc.id_uom_satuan
                LEFT JOIN master_produk_kategori dd
                ON bb.kategori_produk = dd.id_kategori"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) as count 
                        FROM trans_sales_recap_header aa
                        LEFT JOIN master_produk bb
                        ON aa.produk_id = bb.id_produk
                        LEFT JOIN master_produk_uom_satuan cc
                        ON bb.uom_satuan = cc.id_uom_satuan
                        LEFT JOIN master_produk_kategori dd
                        ON bb.kategori_produk = dd.id_kategori"""
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def get_id_trans_kode(self, kode_trans, tahun, bulan):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        # sql_kode = (
        #     f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        # )

        # kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT 
                            LPAD( CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                            CAST ( COALESCE ( MAX ( no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                        FROM trans_sales_recap_header 
                        WHERE DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        number_report = (
            "ALL"
            + "."
            + kode_trans
            + "."
            + str(tahun)
            + "."
            + str(str(bulan).zfill(2) + "." + no_urut[0]["current_no_urut_convert"])
        )
        # print(id_trans)

        data_kode = {
            "number_report": number_report,
            "no_urut": no_urut[0]["current_no_urut_convert"],
        }

        return data_kode

    async def create(self, data):
        tanggal = datetime.today()
        date = datetime.strptime(data["tanggal"], "%Y-%m-%d")
        tahun = date.year
        bulan = date.month

        data_kode = await self.get_id_trans_kode("RPT", tahun, bulan)

        data.update(
            {
                "tanggal": tanggal,
                "tahun": tahun,
                "bulan": bulan,
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
                "number_report": data_kode["number_report"],
                "no_urut": data_kode["no_urut"],
            }
        )

        sqlString = self.db.genStrInsertSingleObject(data, "trans_sales_recap_header")

        try:
            # print(sqlString)
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            print(e)
            message = {"status": "error : " + str(e)}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def update(self, data, data_where):
        tanggal = datetime.today()
        date = datetime.strptime(data["tanggal"], "%Y-%m-%d")
        tahun = date.year
        bulan = date.month

        data.update(
            {
                "tanggal": tanggal,
                "tahun": tahun,
                "bulan": bulan,
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlString = self.db.genUpdateObject(
            data, data_where, "trans_sales_recap_header"
        )
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "trans_sales_recap_header")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def release(self, data):
        data_where = data["data_where"]

        sql_update_st_release_recap_header = f"""UPDATE trans_sales_recap_header SET status_release = 'true'
                                    WHERE id = '{data_where['id']}'"""

        sql_update_invoice = f"""UPDATE trans_inventory_subsidiary_invoice 
                                SET id_sales_report = '{data_where['number_report']}' 
                                WHERE id_sales_report IS NULL AND to_char(tanggal_invoice,'yyyy') = '{data_where['tahun']}' 
                                AND to_char(tanggal_invoice,'MM')::INTEGER = '{data_where['bulan']}'::INTEGER"""

        sql_insert_recap_detail = f"""INSERT INTO trans_sales_recap_detail (id_header,invoice_number) 
                               SELECT '{data_where['id']}',id_trans FROM trans_inventory_subsidiary_invoice
                               where id_sales_report = '{data_where['number_report']}'"""

        # sql_insert_sales_recap_inventory = f"""INSERT INTO trans_sales_recap_inventory_detail (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,id_sales_report)
        #                 SELECT produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,'{data_where['number_report']}'
        #                 FROM trans_inventory_detail WHERE  harga_total <> 0 and produk_id = {data_where['produk_id']}"""

        trans = await self.db.executeTrans(
            [
                sql_update_st_release_recap_header,
                sql_update_invoice,
                sql_insert_recap_detail,
            ]
        )

        if trans["status"] == False:
            message = {"status": False, "msg": "Eror. Cek query."}
            print(str(trans["detail"]))
            raise HTTPException(400, str(trans["detail"]))

        # sql_sales_recap_report_detail = f"""
        #         SELECT A
        #         .number_report,
        #         A.tanggal,
        #         A.produk_id,
        #         concat ( b.nama_produk, ' (', C.uom_satuan, ')' ) nama_produk
        #         FROM
        #         trans_sales_recap_header
        #         A LEFT JOIN master_produk b ON A.produk_id = b.id_produk
        #         LEFT JOIN master_produk_uom_satuan C ON B.uom_satuan = C.id_uom_satuan
        #         WHERE A.id = '{data_where['id']}'
        # """

        sql_resume_sale = f"""SELECT
                XX.*,
                AA.nama_produk,
                BB.uom_satuan,
                round( sales_total / sales_qty, 2 ) :: FLOAT AS harga_sat_penj,
                round( hpp / sales_qty, 2 ) :: FLOAT AS harga_sat_hpp,
                sales_total - hpp AS margin_total,
                round( ( sales_total - hpp ) / sales_total * 100, 2 ) :: FLOAT margin_percent 
                FROM
                (
                    SELECT SUM
                    ( bb.amount ) sales_total,
                    SUM ( cc.qty ) sales_qty,
                    SUM ( cc.harga_total_hpp ) AS hpp,
                    produk_id 
                    FROM
                    trans_sales_recap_detail aa
                    LEFT JOIN trans_inventory_subsidiary_invoice bb ON aa.invoice_number = bb.id_trans
                    LEFT JOIN trans_inventory_subsidiary_sales_order cc ON bb.id_trans_sales_order = cc.id_trans 
                    WHERE
                    aa.id_header = {data_where['id']}
                    GROUP BY
                produk_id 
                ) xx
            LEFT JOIN master_produk AA on AA.id_produk = XX.produk_id
            LEFT JOIN master_produk_uom_satuan BB on AA.uom_satuan = BB.id_uom_satuan
            """

        sql_resume_inventory = f"""SELECT
                            t.produk_id,
                            SUM(t.qty_in - t.qty_out) AS inv_qty,
                            SUM(t.ht_in - t.ht_out) AS total_hpp,
                            p.uom_satuan,
                            p.nama_produk,
                            ROUND(
                                SUM(t.ht_in - t.ht_out)
                                / NULLIF(SUM(t.qty_in - t.qty_out), 0),
                                2
                            ) AS harga_satuan
                        FROM (
                            SELECT
                                produk_id,
                                SUM(qty) FILTER (WHERE in_out = 'IN')  AS qty_in,
                                SUM(qty) FILTER (WHERE in_out = 'OUT') AS qty_out,
                                SUM(harga_total) FILTER (WHERE in_out = 'IN')  AS ht_in,
                                SUM(harga_total) FILTER (WHERE in_out = 'OUT') AS ht_out
                            FROM trans_inventory_detail_mutasi
                            WHERE date_part('month', tanggal) <= {data_where['bulan']} and date_part('year', tanggal) <= {data_where['tahun']}
                            GROUP BY produk_id
                        ) t
                        LEFT JOIN master_produk p
                            ON p.id_produk = t.produk_id
                        GROUP BY
                            t.produk_id,
                            p.nama_produk,
                            p.uom_satuan;
                            """

        sql_detail_sales = f"""	SELECT aa.invoice_number,
                                    dd.nama_customer,
                                    ee.cabang_name,
                                    ff.company_name,
                                    gg.nama_produk,
                                    cc.qty,
                                    hh.uom_satuan,
                                    cc.harga_satuan,
                                    cc.harga_total,
                                    cc.harga_satuan_hpp,
                                    cc.harga_total_hpp,
                                    cc.harga_total-cc.harga_total_hpp as margin,
                                    round(((cc.harga_total-cc.harga_total_hpp)*100/cc.harga_total::FLOAT)::NUMERIC,2) as percent_margin
                                    FROM trans_sales_recap_detail aa
                                    LEFT JOIN trans_inventory_subsidiary_invoice bb on aa.invoice_number=bb.id_trans
                                    LEFT JOIN trans_inventory_subsidiary_sales_order cc on bb.id_trans_sales_order=cc.id_trans
                                    LEFT JOIN master_customer dd on bb.customer_id=dd.id_customer
                                    LEFT JOIN master_company_cabang ee on cc.cabang_id=ee.id_cabang
                                    LEFT JOIN master_company ff on cc.company_id=ff.id_company
                                    LEFT JOIN master_produk gg on cc.produk_id=gg.id_produk
                                    LEFT JOIN master_produk_uom_satuan hh on gg.uom_satuan=hh.id_uom_satuan
                                    WHERE id_header={data_where['id']}"""

        sql_detail_inventory = f"""SELECT
            C.company_name,
            D.cabang_name,
            A.qty_in - qty_out AS qty,
            CASE
                WHEN ( qty_in - qty_out ) = 0 THEN
                0 ELSE ROUND( ( ht_in - ht_out ) / ( qty_in - qty_out ), 2 ) 
            END AS harga_satuan,
            ht_in - ht_out AS harga_total
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
            WHERE date_part('month', tanggal) <= {data_where['bulan']} and date_part('year', tanggal) <= {data_where['tahun']}
            GROUP BY
            produk_id,
            company_id,
            cabang_id 
            ) A
            LEFT JOIN master_company C ON A.company_id = C.id_company
            LEFT JOIN master_company_cabang D on A.company_id = D.id_company AND D.id_cabang = A.cabang_id
            WHERE qty_in - qty_out != 0
            ORDER BY A.cabang_id, A.produk_id"""

        # sales_recap_report_detail = await self.db.executeToDict(
        #     sql_sales_recap_report_detail
        # )
        resume_sale = await self.db.executeToDict(sql_resume_sale)
        resume_inventory = await self.db.executeToDict(sql_resume_inventory)
        detail_sales = await self.db.executeToDict(sql_detail_sales)
        detail_inventory = await self.db.executeToDict(sql_detail_inventory)

        if trans["status"] == False:
            message = {"status": False, "msg": "Eror. Cek query."}
            print(str(trans["detail"]))
            raise HTTPException(400, str(trans["detail"]))

        pdf = PDF(
            number_report=data_where["number_report"],
            resume_sale_data=resume_sale,
            resume_inventory_data=resume_inventory,
            detail_sales_data=detail_sales,
            detail_inventory_data=detail_inventory,
        )
        pdf.generate_report()

    async def get_rekap_resume(self, id_header, id_sales_report):

        return await self.stream_file(
            f"files/sales_order_report/{id_sales_report}.pdf",
            f"{id_sales_report}.pdf",
        )

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
url/api/c_sales_order_recap/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_report/c_sales_order_recap/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_sales_order_recap()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_report/c_sales_order_recap/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_sales_order_recap()
    return await ob_data.create(data)


@app.post("/api/f_report/c_sales_order_recap/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_sales_order_recap()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_report/c_sales_order_recap/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_sales_order_recap()
    return await ob_data.delete(data)


@app.post("/api/f_report/c_sales_order_recap/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_sales_order_recap()
    return await ob_data.release(data)


@app.get("/api/f_report/c_sales_order_recap/get_rekap_resume")
async def get_rekap_resume(id, number_report: str):
    ob_data = c_sales_order_recap()
    return await ob_data.get_rekap_resume(id, number_report)


@app.get("/api/f_report/c_sales_order_recap/get_rekap_resume_report")
async def get_rekap_resume_report(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_sales_order_recap()
    path_parent = params.loc["file_sales_recap_report"]
    path = path_parent + id_trans + ".pdf"
    return await ob_data.stream_file(path, id_trans)
