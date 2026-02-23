import base64
from datetime import datetime

from fastapi import HTTPException, Request

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_inventory_detail_bad_stock_release(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def release(self, data):
        status_release = await self.validate_release(data["id_trans"])
        if status_release > 0:
            return {
                "status": "Success",
                "detail": "Transaksi sudah direlease sebelumnya",
            }

        ## validasi stok inventory dan jumlah quantity yang akan dirilis
        await self.validasi_quantity(data["id_trans"])
        sql_update_status_release = self.update_status_release(data)
        sql_insert_mutasi_in = self.insert_mutasi_in(data)
        sql_insert_mutasi_out = self.insert_mutasi_out(data)
        sql_delete_inventory_detail = self.delete_inventory_detail(data)
        sql_insert_inventory_detail_good = self.insert_inventory_detail_good(data)
        sql_insert_inventory_detail_bad = self.insert_inventory_detail_bad(data)

        try:

            trans = await self.db.executeTrans(
                [
                    sql_update_status_release,
                    sql_insert_mutasi_in,
                    sql_insert_mutasi_out,
                    sql_delete_inventory_detail,
                    sql_insert_inventory_detail_good,
                    sql_insert_inventory_detail_bad,
                ]
            )

            if trans["status"] == False:
                raise HTTPException(400, str(trans["detail"]))

            return {"status": "success", "detail": "data berhasil dirilis"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    async def validate_release(self, id_trans):
        sql = f"""SELECT count(*) count FROM trans_inventory_detail_bad_stock_header WHERE id_trans = '{id_trans}' AND status_release = TRUE"""
        res = await self.db.executeToDict(sql)
        result = res[0]["count"]
        return result

    async def validasi_quantity(self, id_trans):
        sql_validate = f"""
                SELECT
                aa.company_id,
                aa.produk_id,
                aa.cabang_id,
                bb.nama_produk,
                SUM(aa.qty) as qty_sales,
                SUM(COALESCE(cc.qty,0)) as qty_inventory
                FROM
                ( 
                    SELECT company_id, produk_id, cabang_id, qty 
                    FROM trans_inventory_detail_bad_stock A
                    LEFT JOIN trans_inventory_detail_bad_stock_header B on A.id_trans = B.id_trans
                    WHERE A.id_trans = '{id_trans}' 
                ) aa
                LEFT JOIN master_produk bb ON aa.produk_id = bb.id_produk
                LEFT JOIN trans_inventory_detail as cc ON cc.produk_id = aa.produk_id  AND cc.company_id = aa.company_id  AND cc.cabang_id = aa.cabang_id
                GROUP BY 
                aa.company_id,
                aa.produk_id,
                aa.cabang_id,
                bb.nama_produk
                HAVING SUM(aa.qty) > SUM(COALESCE(cc.qty,0))

            """

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
                    string = f"""Produk {res['nama_produk']} memiliki sisa stok {res['qty_inventory']}, """
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

    def insert_mutasi_out(self, data):
        sql_insert_mutasi = f"""
            INSERT INTO trans_inventory_detail_mutasi (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate,in_out,mutasi_type,id_references,tabel_reference,tanggal)
            (

            SELECT A
                .produk_id,
                B.company_id,
                B.cabang_id,
                A.qty,
                A.harga_satuan_hpp,
                A.harga_total,
                '{datetime.today()}' as updateindb,
                '{auth.AuthAction.get_data_params("username")}' as userupdate,
                'OUT' as in_out,
                'BS' as mutasi_type,
                A.id_trans as id_references,
                'trans_inventory_detail_bad_stock' as tabel_reference,
                '{datetime.now().date()}' as tanggal
                FROM
                trans_inventory_detail_bad_stock
                A LEFT JOIN trans_inventory_detail_bad_stock_header B ON A.id_trans = B.id_trans
                WHERE B.company_id = {data['company_id']}
                AND B.cabang_id = {data['cabang_id']}
                AND A.id_trans = '{data['id_trans']}'
            )"""

        return sql_insert_mutasi

    def insert_mutasi_in(self, data):
        sql_insert_mutasi = f"""
            INSERT INTO trans_inventory_detail_mutasi (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate,in_out,mutasi_type,id_references,tabel_reference,tanggal, stock_condition)
            (

            SELECT A
                .produk_id,
                B.company_id,
                B.cabang_id,
                A.qty,
                A.harga_satuan_hpp,
                A.harga_total,
                '{datetime.today()}' as updateindb,
                '{auth.AuthAction.get_data_params("username")}' as userupdate,
                'IN' as in_out,
                'BS' as mutasi_type,
                A.id_trans as id_references,
                'trans_inventory_detail_bad_stock' as tabel_reference,
                '{datetime.now().date()}' as tanggal,
                'bad' as stock_condition
                FROM
                trans_inventory_detail_bad_stock
                A LEFT JOIN trans_inventory_detail_bad_stock_header B ON A.id_trans = B.id_trans
                WHERE B.company_id = {data['company_id']}
                AND B.cabang_id = {data['cabang_id']}
                AND A.id_trans = '{data['id_trans']}'
            )"""

        return sql_insert_mutasi

    def update_status_release(self, data):
        sql_update_status_release_inv_sales_order = f"""UPDATE trans_inventory_detail_bad_stock_header SET status_release = 'true'
                WHERE id_trans = '{data['id_trans']}'"""
        return sql_update_status_release_inv_sales_order

    def delete_inventory_detail(self, data):
        sql_delete_inv_detail = f"""DELETE FROM trans_inventory_detail 
        WHERE company_id = {data['company_id']} AND cabang_id = {data['cabang_id']} AND produk_id IN(
            SELECT produk_id
            FROM trans_inventory_detail_bad_stock 
			WHERE id_trans = '{data['id_trans']}'
        )"""

        return sql_delete_inv_detail

    def insert_inventory_detail_good(self, data):

        sql_insert_inv_detail_good = f"""insert into trans_inventory_detail (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate, stock_condition) 
        SELECT  produk_id,
                company_id,
                cabang_id, 
                qty_in - qty_out as qty,
                CASE 
                  WHEN (qty_in - qty_out) = 0 THEN 0
                  ELSE ROUND((ht_in - ht_out) / (qty_in - qty_out), 2)
                 END as harga_satuan,
                ht_in - ht_out as harga_total,
                '{datetime.today()}', '{auth.AuthAction.get_data_params("username")}',
                'good' as stock_condition
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
					FROM trans_inventory_detail_bad_stock 
					WHERE id_trans = '{data["id_trans"]}'
                )
                and company_id = {data["company_id"]} 
                and cabang_id = {data["cabang_id"]}
                and stock_condition = 'good'
                GROUP BY
                produk_id,
                company_id,
                cabang_id
                ) aa"""

        # sql_insert_inv_detail_out = self.db.genStrInsertSingleObject(
        #     data_inv_detail_out, "trans_inventory_detail"
        # )

        return sql_insert_inv_detail_good

    def insert_inventory_detail_bad(self, data):
        sql_insert_inv_detail_bad = f"""insert into trans_inventory_detail (produk_id,company_id,cabang_id,qty,harga_satuan,harga_total,updateindb,userupdate, stock_condition) 
        SELECT  produk_id,
                company_id,
                cabang_id, 
                qty_in - qty_out as qty,
                CASE 
                  WHEN (qty_in - qty_out) = 0 THEN 0
                  ELSE ROUND((ht_in - ht_out) / (qty_in - qty_out), 2)
                 END as harga_satuan,
                ht_in - ht_out as harga_total,
                '{datetime.today()}', '{auth.AuthAction.get_data_params("username")}',
                'bad' as stock_condition
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
					FROM trans_inventory_detail_bad_stock 
					WHERE id_trans = '{data["id_trans"]}'
                )
                and company_id = {data["company_id"]} 
                and cabang_id = {data["cabang_id"]}
                and stock_condition = 'bad'
                GROUP BY
                produk_id,
                company_id,
                cabang_id
                ) aa"""
        # sql_insert_inv_detail_out = self.db.genStrInsertSingleObject(
        #     data_inv_detail_out, "trans_inventory_detail"
        # )

        return sql_insert_inv_detail_bad


@app.post("/api/f_trans/c_inventory_detail_bad_stock/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_inventory_detail_bad_stock_release()
    data = data["data_where_update"]
    return await ob_data.release(data)
