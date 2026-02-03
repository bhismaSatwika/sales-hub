import base64
from datetime import datetime, timedelta

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse
from modules.apps.util.paid_payment import paid_payment

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel
from modules.f_trans.sales_order_create_pdf import PDF
from modules.f_trans.delivery_order_create_pdf import PDF as PDF_DO
import hashlib


class c_subsidiary_inventory_sales_order_release(object):
    def __init__(self):
        self.db = Db()
        self.paid_payment = paid_payment()
        self.id_trans_mutasi = ""
        self.detail_data_mutasi = {}
        self.sales_order = {}
        self.data_kode_do = {}

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
                        FROM trans_inventory_subsidiary_sales_order 
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

    async def get_id_trans_kode_release(
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
        self, company_id, cabang_id, produk_id, kode_trans, tahun, bulan
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

    async def create_pdf_so(self, id_trans):
        sql = f"""SELECT
                    aa.id_trans,
                    bb.id_produk AS produk_id,
                    bb.nama_produk,
                    cc.id_kategori AS kategori_id,
                    cc.kategori,
                    dd.id_uom_satuan,
                    dd.uom_satuan,
                    ee.id_company AS company_id,
                    ee.company_name,
                    ff.id_cabang AS cabang_id,
                    ff.cabang_name,
                    aa.qty,
                    aa.harga_satuan,
                    aa.harga_total,
                    ( CASE WHEN aa.status_release = TRUE THEN 'Release' ELSE'' END ) AS ket_status_release,
                    aa.status_release,
                    aa.tanggal,
                    aa.file_upload,
                    gg.id_customer AS customer_id,
                    gg.nama_customer,
                    aa.ppn_percent,
                    aa.ppn_value,
                    aa.pph_22_percent,
                    aa.pph_22_value,
                    aa.harga_total_ppn_pph,
                    aa.no_urut,
                    aa.updateindb,
                    gg.alamat,
                    gg.no_ktp,
                    gg.no_hp,
                    gg.email,
                    gg.account_va,
                    gg.account_bank_name,
                    hh.id_trans as no_invoice,
                    bb.ppn,
                    bb.pph22,
                    ii.username as nik,
					ii.name as nama_sales,
                    hh.amount_total_outstanding
                    aa.id_pembayaran,
                    jj.pembayaran,
                    aa.biaya_admin
                FROM
                    trans_inventory_subsidiary_sales_order aa
                    LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                    LEFT JOIN master_produk_kategori cc ON bb.kategori_produk = cc.id_kategori
                    LEFT JOIN master_produk_uom_satuan dd ON bb.uom_satuan = dd.id_uom_satuan
                    LEFT JOIN master_company ee ON aa.company_id = ee.id_company
                    LEFT JOIN master_company_cabang ff ON aa.cabang_id = ff.id_cabang 
                    AND aa.company_id = ff.id_company
                    LEFT JOIN master_customer gg ON aa.customer_id = gg.id_customer
                    LEFT JOIN trans_inventory_subsidiary_invoice hh ON aa.id_trans = hh.id_trans_sales_order
                    LEFT JOIN (select * from master_user where is_salesman = 't') ii ON aa.salesman = ii.id_user
                    LEFT JOIN master_jenis_pembayaran jj ON aa.id_pembayaran = jj.id_pembayaran
                    WHERE aa.id_trans = '{id_trans}'"""

        result = await self.db.executeToDict(sql)

        data = result[0]
        pdf = PDF(data)
        # print(data)
        pdf.generate_report()

        # return data

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
                            LEFT JOIN master_company_cabang cc ON aa.company_id = cc.id_company AND aa.cabang_id = cc.id_cabang
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
                            LEFT JOIN master_company_cabang cc ON aa.company_id = cc.id_company 
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

    async def create_pdf_do_old(self, id_trans):
        sql = f"""SELECT
                    aa.id_trans,
                    aa.id_trans_sales_order,
                    aa.tanggal_do,
                    bb.customer_id,
                    cc.nama_produk,
                    dd.id_kategori AS kategori_id,
                    dd.kategori,
                    ee.id_uom_satuan,
                    ee.uom_satuan,
                    ff.id_company AS company_id,
                    ff.company_name,
                    gg.id_cabang AS cabang_id,
                    gg.cabang_name,
                    bb.qty,
                    bb.harga_satuan,
                    bb.harga_total,
                    hh.id_customer AS customer_id,
                    hh.nama_customer,
                    hh.alamat,
                    hh.no_ktp,
                    hh.no_hp,
                    hh.email,
                    cc.ppn,
                    cc.pph22 
                FROM
                    trans_inventory_subsidiary_delivery_order aa
                    LEFT JOIN trans_inventory_subsidiary_sales_order bb ON aa.id_trans_sales_order = bb.id_trans
                    LEFT JOIN master_produk cc ON bb.produk_id = cc.id_produk
                    LEFT JOIN master_produk_kategori dd ON cc.kategori_produk = dd.id_kategori
                    LEFT JOIN master_produk_uom_satuan ee ON cc.uom_satuan = ee.id_uom_satuan
                    LEFT JOIN master_company ff ON bb.company_id = ff.id_company
                    LEFT JOIN master_company_cabang gg ON bb.cabang_id = gg.id_cabang 
                    AND bb.company_id = gg.id_company
                    LEFT JOIN master_customer hh ON bb.customer_id = hh.id_customer
                    WHERE bb.id_trans = '{id_trans}'"""

        result = await self.db.executeToDict(sql)

        data = result[0]
        pdf = PDF_DO(data)
        # print(data)
        pdf.generate_report()

        return data

    async def validate_release(self, id_trans):
        sql = f"""SELECT count(*) count FROM trans_inventory_subsidiary_sales_order_header WHERE id_trans = '{id_trans}' AND status_release = TRUE"""
        res = await self.db.executeToDict(sql)
        result = res[0]["count"]
        return result

    async def release(self, data):
        ## validasi transaksi sudah direlease/blm
        status_release = await self.validate_release(data["id_trans"])
        if status_release > 0:
            return {
                "status": "Success",
                "detail": "Transaksi sudah direlease sebelumnya",
            }

        ## validasi stok inventory dan jumlah quantity yang akan dirilis
        await self.validasi_quantity(
            data["company_id"], data["cabang_id"], data["id_trans"]
        )

        ## validasi payment
        await self.paid_payment.validasi_paid_payment(data)

        # Mengambil data sales order untuk diinsert ke tabel mutasi
        # save id dari insert mutasinya
        sql_insert_mutasi = await self.insert_mutasi(data)

        await self.select_mutasi(data)
        # ---

        # # update status release sales order menjadi true
        sql_update_release_sales_order = self.update_status_release(data)
        # # update status release sales order menjadi true
        sql_update_hpp_sales_order = self.update_hpp(data)
        # # delete inventory detail
        sql_delete_inventory_detail = self.delete_inventory_detail(data)
        # # insert mutasi out dari sales order
        sql_insert_inventory_detail = self.insert_inventory_detail(data)
        # # # insert data sales order ke delivery order
        sql_insert_delivery_order = await self.insert_delivery_order(data)
        # # # insert data sales order ke invoice order
        sql_insert_invoice_order = await self.insert_invoice_order(data)

        try:
            print("\n\n\n\n\n")
            print(sql_insert_mutasi)
            print("\n\n\n\n\n")
            print(sql_update_release_sales_order)
            print("\n\n\n\n\n")
            print(sql_update_hpp_sales_order)
            print("\n\n\n\n\n")
            print(sql_delete_inventory_detail)
            print("\n\n\n\n\n")
            print(sql_insert_inventory_detail)
            print("\n\n\n\n\n")
            print(sql_insert_delivery_order)
            print("\n\n\n\n\n")
            print(sql_insert_invoice_order)

            trans = await self.db.executeTrans(
                [
                    sql_insert_mutasi,
                    sql_update_release_sales_order,
                    sql_update_hpp_sales_order,
                    sql_delete_inventory_detail,
                    sql_insert_inventory_detail,
                    sql_insert_delivery_order,
                    sql_insert_invoice_order,
                ]
            )

            if trans["status"] == False:
                raise HTTPException(400, str(trans["detail"]))

            # await self.create_pdf_so(data["id_trans"])
            await self.create_pdf_do(data["id_trans"])

            return {"status": "success", "detail": "data berhasil dirilis"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    async def validasi_quantity(self, company_id, cabang_id, id_trans):
        sql_validate = f"""SELECT aa.company_id,aa.cabang_id,aa.produk_id,bb.nama_produk,SUM(aa.qty) as qty_inventory,SUM(cc.qty) as qty_sales
                                        FROM trans_inventory_detail aa
                                        LEFT JOIN master_produk bb
                                        ON aa.produk_id = bb.id_produk
                                        LEFT JOIN (
                                        SELECT company_id,produk_id,cabang_id,qty
                                                FROM trans_inventory_subsidiary_sales_order 
                                                WHERE id_trans = '{id_trans}'
                                        ) cc ON aa.produk_id = cc.produk_id AND aa.company_id = cc.company_id AND aa.cabang_id = cc.cabang_id
                                        WHERE aa.company_id = {company_id} AND aa.cabang_id = {cabang_id} 
                                        AND aa.produk_id IN(
                                                SELECT produk_id
                                                FROM trans_inventory_subsidiary_sales_order 
                                                WHERE id_trans = '{id_trans}'
                                        )
                                        GROUP BY aa.company_id,aa.cabang_id,aa.produk_id,bb.nama_produk
                                        HAVING SUM(cc.qty) > SUM(aa.qty)"""

        # print("\n\n\n")
        # print(sql_validate)

        message = ""

        try:
            result = await self.db.executeToDict(sql_validate)
            # print("\n\n\n")
            # print(result)

            if len(result) > 0:
                string = ""
                for res in result:
                    string = f"""Produk {res['nama_produk']} memiliki sisa stok :{res['qty_inventory']}, """
                    message = message + string
                print(string)
                raise HTTPException(
                    status_code=400,
                    detail=string,
                )

        except Exception as e:
            message = "Error ketika melakukan validasi stok: " + message + str(e)
            raise HTTPException(
                status_code=400,
                detail=message,
            )

    async def insert_mutasi(self, data):
        sql_insert_mutasi = f"""
            INSERT INTO trans_inventory_detail_mutasi (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate,in_out,mutasi_type,id_references,tabel_reference,tanggal)
            (

            SELECT
                A.produk_id,
                A.company_id,
                A.cabang_id,
                A.qty,
                b.harga_satuan,
                ROUND(b.harga_satuan * A.qty, 2) as harga_total_hpp,
                '{datetime.today()}' as updateindb,
                '{auth.AuthAction.get_data_params("username")}' as userupdate,
                'OUT' as in_out,
                'SO' as mutasi_type,
                A.id_trans as id_references,
                'trans_inventory_subsidiary_sales_order' as tabel_reference,
                '{datetime.now().date()}' as tanggal
                FROM
                trans_inventory_subsidiary_sales_order A
                LEFT JOIN trans_inventory_detail B on A.produk_id = B.produk_id and A.company_id = B.company_id AND A.cabang_id = B.cabang_id
                WHERE A.company_id = {data['company_id']}
                AND A.cabang_id = {data['cabang_id']}
                AND A.id_trans = '{data['id_trans']}'
            )"""

        return sql_insert_mutasi

    def update_hpp(self, data):
        sql_update_hpp = f"""
            UPDATE trans_inventory_subsidiary_sales_order A 
                SET harga_satuan_hpp = B.harga_satuan,
                harga_total_hpp = ROUND( A.qty * B.harga_satuan, 2 ),
                updateindb = now()
                FROM
                trans_inventory_detail B 
            WHERE
            A.id_trans = '{data['id_trans']}' 
            AND A.produk_id = B.produk_id 
            AND A.company_id = B.company_id 
            AND A.cabang_id = B.cabang_id
        """
        return sql_update_hpp

    async def select_mutasi(self, data):
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
            WHERE company_id = {data['company_id']} and cabang_id = {data['cabang_id']} and produk_id IN (
                    SELECT produk_id
                    FROM trans_inventory_subsidiary_sales_order 
                    WHERE id_trans = '{data['id_trans']}'
                )
                GROUP BY
                produk_id,
                company_id,
                cabang_id
                ) aa"""

        data_detai_mutasi = await self.db.executeToDict(sql_detail_mutasi)
        self.detail_data_mutasi = data_detai_mutasi[0]
        print("\n\n\n\n\n")
        print("Mutasi Data:")
        print(sql_detail_mutasi)
        print(self.detail_data_mutasi)
        print("\n\n\n\n\n")

    def update_status_release(self, data):
        sql_update_status_release_inv_sales_order = f"""UPDATE trans_inventory_subsidiary_sales_order_header SET status_release = 'true'
                WHERE id_trans = '{data['id_trans']}'"""
        return sql_update_status_release_inv_sales_order

    def delete_inventory_detail(self, data):
        sql_delete_inv_detail = f"""DELETE FROM trans_inventory_detail 
        WHERE company_id = {data['company_id']} AND cabang_id = {data['cabang_id']} AND produk_id IN(
            SELECT produk_id
            FROM trans_inventory_subsidiary_sales_order 
			WHERE id_trans = '{data['id_trans']}'
        )"""

        # print("\n\n\n\n\n")
        # print("Delete Inventory Detail")
        # print(sql_delete_inv_detail)
        # print("\n\n\n\n\n")

        return sql_delete_inv_detail

    def insert_inventory_detail(self, data):
        # data_inv_detail_out = {
        #     "produk_id": self.detail_data_mutasi["produk_id"],
        #     "company_id": self.detail_data_mutasi["company_id"],
        #     "cabang_id": self.detail_data_mutasi["cabang_id"],
        #     "qty": int(self.detail_data_mutasi["qty"]),
        #     "harga_satuan": int(self.detail_data_mutasi["harga_satuan"]),
        #     "harga_total": int(self.detail_data_mutasi["harga_total"]),
        #     "updateindb": datetime.today(),
        #     "userupdate": auth.AuthAction.get_data_params("username"),
        # }
        # print("\n\n\n\n\n")
        # print("Inventory Detail")
        # # print(data_inv_detail_out)
        # print("\n\n\n\n\n")

        sql_insert_inv_detail_out = f"""insert into trans_inventory_detail (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate) 
        SELECT  produk_id,
                company_id,
                cabang_id, 
                qty_in - qty_out as qty,
                CASE 
                  WHEN (qty_in - qty_out) = 0 THEN 0
                  ELSE ROUND((ht_in - ht_out) / (qty_in - qty_out), 2)
                 END as harga_satuan,
                ht_in - ht_out as harga_total,
                '{datetime.today()}', '{auth.AuthAction.get_data_params("username")}'
                FROM (
                SELECT
                produk_id,
                company_id,
                cabang_id,
                SUM ( CASE WHEN in_out = 'IN' THEN qty ELSE 0 END) qty_in,
                SUM ( CASE WHEN in_out = 'OUT' THEN qty ELSE 0 END) qty_out,
                SUM ( CASE WHEN in_out = 'IN' THEN harga_total ELSE 0 END) ht_in,
                SUM ( CASE WHEN in_out = 'OUT' THEN harga_total ELSE 0 END) ht_out
                FROM
                trans_inventory_detail_mutasi 
                WHERE
                produk_id IN (
                	SELECT produk_id
					FROM trans_inventory_subsidiary_sales_order 
					WHERE id_trans = '{data["id_trans"]}'
                )
                and company_id = {self.detail_data_mutasi["company_id"]} 
                and cabang_id = {self.detail_data_mutasi["cabang_id"]}
                GROUP BY
                produk_id,
                company_id,
                cabang_id
                ) aa"""

        # sql_insert_inv_detail_out = self.db.genStrInsertSingleObject(
        #     data_inv_detail_out, "trans_inventory_detail"
        # )

        return sql_insert_inv_detail_out

    async def insert_delivery_order(self, data):
        tanggal = datetime.today()
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode_do = await self.get_id_trans_kode_release(
            data["company_id"], data["cabang_id"], "DO", tahun, bulan
        )

        self.data_kode_do = data_kode_do

        sql_inv_sales_order = f"""SELECT * FROM trans_inventory_subsidiary_sales_order_header WHERE id_trans = '{data['id_trans']}'"""
        result_inv_sales_order = await self.db.executeToDict(sql_inv_sales_order)
        sales_order = result_inv_sales_order[0]
        self.sales_order = sales_order

        data_inv_delivery_order = {
            "updateindb": datetime.today(),
            "userupdate": auth.AuthAction.get_data_params("username"),
            "status_release": False,
            "tanggal_do": tanggal,
            "id_trans_sales_order": sales_order["id_trans"],
            "id_trans": data_kode_do["id_trans"],
            "no_urut": data_kode_do["no_urut"],
        }

        sql_insert_inv_delivery_order = self.db.genStrInsertSingleObject(
            data_inv_delivery_order, "trans_inventory_subsidiary_delivery_order"
        )

        return sql_insert_inv_delivery_order

    async def insert_invoice_order(self, data):
        tanggal = datetime.today()
        tahun = tanggal.year
        bulan = tanggal.month

        data_kode_iv = await self.get_id_trans_kode_invoice(
            data["company_id"],
            data["cabang_id"],
            self.detail_data_mutasi["produk_id"],
            "INV",
            tahun,
            bulan,
        )

        new_date = tanggal + timedelta(days=7)
        id_trans_md5 = hashlib.md5(data_kode_iv["id_trans"].encode()).hexdigest()

        print("\n\n")
        print(self.sales_order)
        print("\n\n")

        if int(self.sales_order["id_pembayaran"]) == 1:
            new_date = tanggal + timedelta(days=1)

        data_invoice = {
            "id_trans": data_kode_iv["id_trans"],
            "updateindb": datetime.today(),
            "userupdate": auth.AuthAction.get_data_params("username"),
            "status_release": False,
            "tanggal_invoice": tanggal,
            "id_trans_sales_order": self.sales_order["id_trans"],
            "id_trans_delivery_order": self.data_kode_do["id_trans"],
            "status_invoice": True,
            "no_urut": data_kode_iv["no_urut"],
            "tanggal_due_date": new_date,
            "amount": self.sales_order["harga_total"],
            "amount_ppn": self.sales_order["total_ppn"],
            "amount_pph": self.sales_order["total_pph"],
            "md5_file": id_trans_md5,
            "amount_total": self.sales_order["harga_total_ppn_pph"],
            "amount_total_outstanding": self.sales_order["harga_total_ppn_pph"],
            "customer_id": self.sales_order["customer_id"],
            "id_pembayaran": self.sales_order["id_pembayaran"],
            "biaya_admin": self.sales_order["biaya_admin"],
        }

        sql_insert_invoice = self.db.genStrInsertSingleObject(
            data_invoice, "trans_inventory_subsidiary_invoice"
        )
        return sql_insert_invoice


@app.post("/api/f_trans/c_subsidiary_inventory_sales_order/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_subsidiary_inventory_sales_order_release()
    data = data["data_where_update"]
    return await ob_data.release(data)
