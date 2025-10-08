import base64

from fastapi import Query
from fastapi.responses import StreamingResponse

from library import *
import os
from library.router import app
from library.db import Db
from modules.f_report.create_sales_report_percompany import PDF
from pydantic import BaseModel


class c_sales_report(object):
    def __init__(self):
        self.db = Db()

    async def sales_report(self, company_id, cabang_id, produk_id, tanggal):
        if (
            company_id != None
            and cabang_id != None
            and produk_id != None
            and tanggal != None
        ):
            filter_header = f"""WHERE company_id = {company_id} AND cabang_id = {cabang_id} AND bb.produk_id = {produk_id} AND tanggal_invoice <= '{tanggal}'"""
            filter_detail = f"""WHERE bb.company_id = {company_id} AND bb.cabang_id = {cabang_id} AND aa.produk_id = {produk_id} AND aa.tanggal_invoice <= '{tanggal}'"""

            sql_product = f"""SELECT nama_produk from master_produk WHERE id_produk = {produk_id}"""

        sql_header = (
            f"""SELECT *,
                        round(sales_total/sales_qty,2)::FLOAT as harga_sat_penj,
                        round(hpp/sales_qty,2)::FLOAT as harga_sat_hpp,
                        sales_total-hpp as margin_total,
                    round((sales_total-hpp)/sales_total*100,2)::FLOAT margin_percent
                    FROM
                    (SELECT
                        sum(aa.amount_total) sales_total,
                        sum(aa.qty) sales_qty,
                        sum(bb.harga_total_hpp) as hpp
                    FROM trans_inventory_subsidiary_invoice aa
                    LEFT JOIN trans_inventory_subsidiary_sales_order bb on aa.
                    id_trans_sales_order=bb.id_trans 
                    
                    """
            + filter_header
            + """) xx"""
        )

        print(sql_header)

        sql_detail = (
            f"""SELECT
                            aa.id_trans AS invoice_number
                            ,bb.nama_customer
                            ,hh.cabang_name
                            ,gg.company_name
                            ,aa.qty
                            ,ff.uom_satuan
                            ,ee.harga_satuan
                            ,ee.harga_total
                            ,ee.harga_satuan_hpp
                            ,ee.harga_total_hpp
                            ,ee.harga_total-ee.harga_total_hpp as margin
                            ,round(((ee.harga_total-ee.harga_total_hpp)*100/ee.harga_total::FLOAT)::NUMERIC,2) as percent_margin
                        FROM
                            trans_inventory_subsidiary_invoice aa
                            LEFT JOIN master_customer bb ON aa.customer_id = bb.id_customer
                            LEFT JOIN master_produk cc ON aa.produk_id = cc.id_produk
                            LEFT JOIN master_jenis_pembayaran dd ON aa.id_pembayaran = dd.id_pembayaran
                            LEFT JOIN trans_inventory_subsidiary_sales_order ee ON aa.id_trans_sales_order = ee.id_trans
                            LEFT JOIN master_produk_uom_satuan ff ON cc.uom_satuan = ff.id_uom_satuan
                            LEFT JOIN master_company gg ON ee.company_id = gg.id_company
                            LEFT JOIN master_company_cabang hh ON ee.cabang_id = hh.id_cabang
                            LEFT JOIN ( SELECT id_user, NAME FROM master_user WHERE is_salesman = 't' ) ii ON ee.salesman = ii.id_user
                            LEFT JOIN master_provinsi jj ON bb.kode_prov = jj.kode_prov
                            LEFT JOIN master_produk_kategori kk ON cc.kategori_produk = kk.id_kategori """
            + filter_detail
        )

        query_sql_product = await self.db.executeToDict(sql_product)
        query_sql_header = await self.db.executeToDict(sql_header)
        query_sql_detail = await self.db.executeToDict(sql_detail)

        pdf = PDF(
            product_name=query_sql_product,
            detail_sales_data=query_sql_detail,
            resume_sale_data=query_sql_header,
        )
        pdf_buffer = pdf.generate_report()

        return StreamingResponse(
            pdf_buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename=report.pdf"},
        )


@app.get("/api/f_report/c_sales_report/get_sales_report")
async def get_sales_report(
    company_id: int = Query(None, alias="company_id"),
    cabang_id: int = Query(None, alias="cabang_id"),
    produk_id: int = Query(None, alias="produk_id"),
    tanggal: str = Query(None, alias="tanggal"),
):
    ob_data = c_sales_report()
    return await ob_data.sales_report(company_id, cabang_id, produk_id, tanggal)
