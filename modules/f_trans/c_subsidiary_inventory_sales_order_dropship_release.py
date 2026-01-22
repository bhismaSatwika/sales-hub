import base64
from datetime import datetime, timedelta
import hashlib

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from library import *
import os
from library.router import app
from library.db import Db
from modules.f_trans.delivery_order_create_pdf import PDF as PDF_DO


class c_subsidiary_inventory_sales_order_dropship_release(object):
    def __init__(self):
        self.db = Db()
        self.invoice_pre_payment = {}
        self.data_kode_do = {}

    async def receive(self, data):
        await self.check_payment(data["id_trans"])
        sql_insert_mutasi = self.insert_mutasi(data["id_trans"])
        sql_update_delivery = self.update_delivery(data["id_delivery"])
        await self.get_pre_payment_data(data["id_trans"])
        sql_insert_do = await self.insert_delivery_order(data)
        sql_insert_invoice = await self.insert_invoice_order()
        sql_update_hpp_so = self.update_hpp(data["id_trans"])
        # print(sql_insert_invoice)

        try:
            trans = await self.db.executeTrans(
                [
                    sql_insert_mutasi,
                    sql_update_delivery,
                    sql_update_hpp_so,
                    sql_insert_do,
                    sql_insert_invoice,
                ]
            )
            print("\n\n", trans)
            if trans["status"] == False:
                raise HTTPException(400, ("The error is: ", str(trans["detail"])))

            await self.create_pdf_do(data["id_trans"])
            return "success"

        except Exception as e:
            print(e)
            raise HTTPException(400, ("The error is: ", str(e)))

    def insert_mutasi(self, id_trans):
        userupdate = auth.AuthAction.get_data_params("username")
        now = datetime.today()
        table_reference = "trans_inventory_holding_delivery_preparation_header"
        tanggal = datetime.now().date()
        updateindb = datetime.today()
        sql = f"""
        INSERT INTO trans_inventory_detail_mutasi (id_references, produk_id, company_id, cabang_id, qty, harga_satuan, harga_total, in_out, mutasi_type, updateindb, userupdate, tabel_reference, tanggal)
         WITH holding AS (
            SELECT 
                h.id_trans, h.id_trans_sales_order, d.produk_id, h.company_id, h.cabang_id, d.qty, ROUND(d.grand_total / d.qty, 2) as harga_satuan,
                d.grand_total as harga_total
            FROM trans_inventory_holding_delivery_preparation_header h
            LEFT JOIN trans_inventory_holding_delivery_preparation d ON h.id_trans = d.id_trans
            WHERE h.id_trans_sales_order = '{id_trans}'
        ),
        subsidiary AS (
            SELECT
                s.id_trans, d.produk_id, s.company_id, s.cabang_id, d.qty, d.harga_satuan,
                d.harga_total
            FROM trans_inventory_subsidiary_sales_order_header s
            LEFT JOIN trans_inventory_subsidiary_sales_order d ON s.id_trans = d.id_trans
            WHERE s.id_trans = '{id_trans}'
        )
        (
            SELECT 
                h.id_trans_sales_order AS id_references, h.produk_id, h.company_id, h.cabang_id, h.qty, h.harga_satuan, 
                h.harga_total, 'OUT' AS in_out, 'SO' AS mutasi_type, '{updateindb}'::TIMESTAMP as updateindb, '{userupdate}' as userupdate, '{table_reference}' as tabel_reference, '{tanggal}'::DATE as tanggal
            FROM holding h
            LEFT JOIN subsidiary s
                ON h.produk_id = s.produk_id
                AND h.id_trans_sales_order = s.id_trans
                AND h.company_id = s.company_id
                AND h.cabang_id = s.cabang_id
            UNION ALL
            SELECT
                h.id_trans_sales_order AS id_references, h.produk_id, h.company_id, h.cabang_id, h.qty, h.harga_satuan, h.harga_total,
                'IN' AS in_out, 'DS' AS mutasi_type, '{updateindb}'::TIMESTAMP as updateindb, '{userupdate}' as userupdate, '{table_reference}' as tabel_reference, '{tanggal}'::DATE as tanggal
            FROM holding h
        )

        UNION ALL
        (
            SELECT 
                h.id_trans_sales_order AS id_references, h.produk_id, 1 AS company_id, 1 AS cabang_id, h.qty,
                h.harga_satuan, h.harga_total, 'OUT' AS in_out, 'DS' AS mutasi_type, '{updateindb}'::TIMESTAMP as updateindb, '{userupdate}' as userupdate, '{table_reference}' as tabel_reference, '{tanggal}'::DATE as tanggal
            FROM holding h
            LEFT JOIN subsidiary s
                ON h.produk_id = s.produk_id
                AND h.id_trans_sales_order = s.id_trans
                AND h.company_id = s.company_id
                AND h.cabang_id = s.cabang_id
            UNION ALL
            SELECT
                h.id_trans_sales_order AS id_references, h.produk_id, 1 AS company_id, 1 AS cabang_id,
                h.qty, h.harga_satuan, h.harga_total, 'IN' AS in_out, 'DS' AS mutasi_type, '{updateindb}'::TIMESTAMP as updateindb, '{userupdate}' as userupdate, '{table_reference}' as tabel_reference, '{tanggal}'::DATE as tanggal
            FROM holding h
        )
        ORDER BY company_id, cabang_id, produk_id, in_out;
        """
        print(sql)
        return sql

    def update_delivery(self, id_trans):
        update_delivery = self.db.genUpdateObject(
            {"is_delivered": True},
            {"id_trans": id_trans},
            "trans_inventory_holding_delivery_preparation_header",
        )

        return update_delivery

    def update_hpp(self, id_trans):
        sql_update_hpp = f"""
           UPDATE trans_inventory_subsidiary_sales_order A
            SET harga_satuan_hpp = ROUND(grand_total / qty, 2), harga_total_hpp = B.grand_total
            FROM (
            SELECT A.id_trans_sales_order, B.produk_id, b.grand_total FROM trans_inventory_holding_delivery_preparation_header A
            LEFT JOIN trans_inventory_holding_delivery_preparation B ON A.id_trans = B.id_trans
            WHERE id_trans_sales_order = '{id_trans}'
            ) B
            WHERE A.id_trans = B.id_trans_sales_order AND a.produk_id = B.produk_id;
        """
        return sql_update_hpp

    async def get_id_trans_kode_do(
        self, company_id, cabang_id, kode_trans, tahun, bulan
    ):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        )
        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT
                                LPAD( CAST ( COALESCE ( MAX ( aa.no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                                CAST ( COALESCE ( MAX ( aa.no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                            FROM
                                trans_inventory_subsidiary_delivery_order aa
                                LEFT JOIN trans_inventory_subsidiary_sales_order bb ON aa.id_trans_sales_order = bb.id_trans 
                            WHERE
                                bb.company_id = {company_id}
                                AND bb.cabang_id = {cabang_id} 
                                AND DATE_PART( 'year', aa.tanggal_do ) = {tahun} 
                                AND DATE_PART( 'month', aa.tanggal_do ) = {bulan}"""
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

    async def get_id_trans_kode_invoice(
        self, company_id, cabang_id, kode_trans, tahun, bulan
    ):
        # bulan = datetime.now().month
        # tahun = datetime.now().year

        sql_kode = (
            f"""SELECT kode FROM master_company WHERE id_company = {company_id}"""
        )

        kode_company = await self.db.executeToDict(sql_kode)

        sql_no_urut = f"""SELECT
                                LPAD( CAST ( COALESCE ( MAX ( A.no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ), 4, '0' ) AS current_no_urut_convert,
                                CAST ( COALESCE ( MAX ( A.no_urut ), 0 ) + 1 AS VARCHAR ( 32 ) ) AS current_no_urut 
                            FROM
                                trans_inventory_subsidiary_invoice A
                                LEFT JOIN trans_inventory_subsidiary_sales_order B on A.id_trans_sales_order = B.id_trans
                            WHERE
                                company_id = {company_id} 
                                AND cabang_id = {cabang_id}
                                AND DATE_PART( 'year', tanggal_invoice ) = {tahun} 
                                AND DATE_PART( 'month', tanggal_invoice ) = {bulan}"""

        no_urut = await self.db.executeToDict(sql_no_urut)
        # print(no_urut[0]['current_no_urut_convert'])

        id_trans = (
            "IDFOOD."
            + str(kode_company[0]["kode"])
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

    async def get_pre_payment_data(self, data):
        sql_invoice_pre_payment = f"""SELECT A.*, B.company_id, B.cabang_id FROM trans_inventory_subsidiary_invoice_pre_payment A
        LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
        WHERE id_trans_sales_order = '{data}'"""
        result_inv_sales_order = await self.db.executeToDict(sql_invoice_pre_payment)
        invoice_pre_payment = result_inv_sales_order[0]
        self.invoice_pre_payment = invoice_pre_payment

    async def check_payment(self, id_trans):
        sql = f"""
                SELECT complete_payment FROM trans_inventory_subsidiary_invoice_pre_payment A
        WHERE id_trans_sales_order = '{id_trans}'
        """
        print(sql)

        result = await self.db.executeToDict(sql)
        res = result[0]["complete_payment"]
        print(result)
        if res == False:
            raise HTTPException(
                400, "Harap lunaskan pembayaran terlebih dahulu untuk order ini"
            )

    async def insert_delivery_order(self, data):
        tanggal = datetime.today()
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode_do = await self.get_id_trans_kode_do(
            self.invoice_pre_payment["company_id"],
            self.invoice_pre_payment["cabang_id"],
            "DO",
            tahun,
            bulan,
        )

        self.data_kode_do = data_kode_do

        data_inv_delivery_order = {
            "updateindb": datetime.today(),
            "userupdate": auth.AuthAction.get_data_params("username"),
            "status_release": False,
            "tanggal_do": tanggal,
            "id_trans_sales_order": self.invoice_pre_payment["id_trans_sales_order"],
            "id_trans": data_kode_do["id_trans"],
            "no_urut": data_kode_do["no_urut"],
        }

        sql_insert_inv_delivery_order = self.db.genStrInsertSingleObject(
            data_inv_delivery_order, "trans_inventory_subsidiary_delivery_order"
        )

        return sql_insert_inv_delivery_order

    async def insert_invoice_order(self):
        tanggal = datetime.today()
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode_iv = await self.get_id_trans_kode_invoice(
            self.invoice_pre_payment["company_id"],
            self.invoice_pre_payment["cabang_id"],
            "INV",
            tahun,
            bulan,
        )

        new_date = tanggal + timedelta(days=7)
        id_trans_md5 = hashlib.md5(data_kode_iv["id_trans"].encode()).hexdigest()

        if int(self.invoice_pre_payment["id_pembayaran"]) == 1:
            new_date = tanggal + timedelta(days=1)

        data_invoice = {
            "id_trans": data_kode_iv["id_trans"],
            "updateindb": datetime.today(),
            "userupdate": auth.AuthAction.get_data_params("username"),
            "status_release": False,
            "tanggal_invoice": tanggal,
            "id_trans_sales_order": self.invoice_pre_payment["id_trans_sales_order"],
            "id_trans_delivery_order": self.data_kode_do["id_trans"],
            "status_invoice": True,
            "no_urut": data_kode_iv["no_urut"],
            "tanggal_due_date": new_date,
            "amount": self.invoice_pre_payment["amount"],
            "amount_ppn": self.invoice_pre_payment["amount_ppn"],
            "amount_pph": self.invoice_pre_payment["amount_pph"],
            "md5_file": id_trans_md5,
            "amount_total": self.invoice_pre_payment["amount_total"],
            "amount_total_outstanding": self.invoice_pre_payment[
                "amount_total_outstanding"
            ],
            "customer_id": self.invoice_pre_payment["customer_id"],
            "id_pembayaran": self.invoice_pre_payment["id_pembayaran"],
            "biaya_admin": self.invoice_pre_payment["biaya_admin"],
            "complete_payment": self.invoice_pre_payment["complete_payment"],
            "reference_pre_payment": self.invoice_pre_payment["id_trans"],
        }

        sql_insert_invoice = self.db.genStrInsertSingleObject(
            data_invoice, "trans_inventory_subsidiary_invoice"
        )
        return sql_insert_invoice

    async def create_pdf_do(self, id_trans):

        sql_header = f"""SELECT
                            ff.id_trans,
                            ff.id_trans_sales_order,
                            ff.tanggal_do,
                            bb.id_company AS company_id,
                            bb.company_name,
                            cc.id_cabang AS cabang_id,
                            cc.cabang_name,
                            gg.id_customer AS customer_id,
                            gg.nama_customer,
                            gg.alamat,
                            gg.no_ktp,
                            gg.no_hp,
                            gg.email,
                            gg.account_va,
                            gg.account_bank_name
                        FROM
                            trans_inventory_subsidiary_sales_order_header aa
                            LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                            LEFT JOIN master_company_cabang cc ON aa.company_id = bb.id_company AND aa.cabang_id = cc.id_cabang
                            LEFT JOIN master_user dd ON aa.salesman = dd.id_user
                            LEFT JOIN master_jenis_pembayaran ee ON aa.id_pembayaran = ee.id_pembayaran
                            LEFT JOIN trans_inventory_subsidiary_delivery_order ff ON aa.id_trans = ff.id_trans_sales_order
                            LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer 	
                        WHERE
                            aa.id_trans = '{id_trans}'"""

        sql_detail = f"""SELECT
                            dd.nama_produk,
                            aa.qty,
                            ee.uom_satuan
                        FROM
                            trans_inventory_subsidiary_sales_order aa
                            LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                            LEFT JOIN master_company_cabang cc ON aa.company_id = bb.id_company 
                            AND aa.cabang_id = cc.id_cabang
                            LEFT JOIN master_produk dd ON aa.produk_id = dd.id_produk
                            LEFT JOIN master_produk_uom_satuan ee ON dd.uom_satuan = ee.id_uom_satuan
                            LEFT JOIN trans_inventory_subsidiary_delivery_order ff ON aa.id_trans = ff.id_trans_sales_order 
                        WHERE
                            aa.id_trans = '{id_trans}'"""

        result_header = await self.db.executeToDict(sql_header)
        result_detail = await self.db.executeToDict(sql_detail)

        data_header = result_header[0]
        data_detail = result_detail
        pdf = PDF_DO(data_header, data_detail)

        pdf_buffer = pdf.generate_report()
        filenamex = data_header["id_trans"]

        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename={filenamex}.pdf"},
        )


@app.post("/api/f_trans/c_subsidiary_inventory_sales_order_dropship_release/receive")
async def receive(request: Request):
    data = await request.json()
    ob_data = c_subsidiary_inventory_sales_order_dropship_release()
    return await ob_data.receive(data)
