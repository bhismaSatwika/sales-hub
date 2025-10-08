import base64

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from modules.f_report.create_sales_report import PDF
from pydantic import BaseModel


class c_sales_report(object):
    def __init__(self):
        self.db = Db()

    async def sales_report(self, company_id, cabang_id, produk_id, tanggal):
        if company_id != None and cabang_id != None and produk_id != None and tanggal !=None:
            filter_header = f"""WHERE company_id = {company_id} AND cabang_id = {cabang_id} AND produk_id = {produk_id} AND tanggal_invoice = '{tanggal}'"""
            filter_detail = f"""WHERE bb.company_id = {company_id} AND bb.cabang_id = {cabang_id} AND bb.produk_id = {produk_id} AND aa.tanggal_invoice = '{tanggal}'"""

        sql_header = f"""SELECT *,
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
                    LEFT JOIN trans_inventory_subsidiary_sales_order bb on aa.id_trans_sales_order=bb.id_trans """+filter_header+""") xx"""

        sql_detail = f"""SELECT
                    aa.id_trans,
                    ee.company_id,
                    gg.company_name,
                    ee.cabang_id,
                    hh.cabang_name,
                    aa.tanggal_invoice,
                    aa.updateindb,
                    aa.id_trans_sales_order,
                    aa.id_trans_delivery_order,
                    aa.tanggal_due_date,
                    aa.produk_id,
                    cc.nama_produk as produk,
                    cc.deskripsi_produk,
                    kk.kategori as kategori_produk,
                    aa.qty,
                    ff.uom_satuan,
                    ee.harga_satuan,
                    ee.harga_total,
                    ee.harga_satuan_hpp,
                    ee.harga_total_hpp,
                    ee.harga_total-ee.harga_total_hpp as margin,
                    round(((ee.harga_total-ee.harga_total_hpp)*100/ee.harga_total::FLOAT)::NUMERIC,2) as percent_margin,
                    ee.ppn_percent,
                    ee.ppn_value,
                    ee.pph_22_percent,
                    ee.pph_22_value,
                    ee.harga_total_ppn_pph,
                    ee.biaya_admin,
                    aa.amount,
                    aa.amount_ppn,
                    aa.amount_pph,
                    aa.amount_total,
                    aa.amount_total_outstanding,
                    CASE 
                        WHEN aa.complete_payment IS TRUE
                            THEN 'Done'
                            ELSE 'Pending'
                        END as complete_payment,
                    dd.pembayaran,
                    ee.salesman as id_salesman,
                    ii.name as salesman,
                    bb.nama_customer,
                    bb.alamat,
                    jj.nama as provinsi,
                    bb.npwp,
                    bb.no_ktp,
                    bb.account_va,
                    bb.account_bank_name
            FROM trans_inventory_subsidiary_invoice aa
            LEFT JOIN master_customer bb ON aa.customer_id = bb.id_customer
            LEFT JOIN master_produk cc ON aa.produk_id = cc.id_produk
            LEFT JOIN master_jenis_pembayaran dd ON aa.id_pembayaran = dd.id_pembayaran
            LEFT JOIN trans_inventory_subsidiary_sales_order ee ON aa.id_trans_sales_order = ee.id_trans
            LEFT JOIN master_produk_uom_satuan ff on cc.uom_satuan=ff.id_uom_satuan
            LEFT JOIN master_company gg on ee.company_id=gg.id_company
            LEFT JOIN master_company_cabang hh on ee.cabang_id=hh.id_cabang
            LEFT JOIN (select id_user,name from master_user where is_salesman='t') ii on ee.salesman = ii.id_user
            LEFT JOIN master_provinsi jj ON bb.kode_prov = jj.kode_prov
            LEFT JOIN master_produk_kategori kk ON cc.kategori_produk = kk.id_kategori """+filter_detail

        query_sql_header = await self.db.executeToDict(sql_header)
        query_sql_detail = await self.db.executeToDict(sql_detail)

       


@app.get("/api/f_report/c_sales_report/testing")
async def get_sales_report(
    company_id: int = Query(None, alias="company_id"),
    cabang_id: int = Query(None, alias="cabang_id"),
    produk_id: int = Query(None, alias="produk_id"),
    tanggal: str = Query(None, alias="tanggal"),
):
    ob_data = c_sales_report()
    return await ob_data.sales_report(company_id, cabang_id, produk_id, tanggal)
