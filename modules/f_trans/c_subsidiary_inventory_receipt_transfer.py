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


class c_subsidiary_inventory_receipt_transfer(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read_pending(
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
            filter_other = f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}' AND zz.status_release = false"
            filter_other_conj = f" and "

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
                        bb.id_company as company_id,
                        bb.company_name,
                        cc.id_cabang as cabang_id,
                        cc.cabang_name,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                        ELSE 'draft'
                        END) as ket_status_release,
                        aa.status_release,
                        aa.tanggal,
                        aa.no_urut,
                        aa.updateindb,
						ee.id_produk as produk_id,
					    ee.nama_produk ||' ('||gg.uom_satuan||')' as nama_produk,
						dd.qty,
						dd.harga_satuan,
						dd.harga_total,
                        dd.transport_cost_total,
						dd.tanggal as tanggal_transfer,
					    gg.id_uom_satuan,
						gg.uom_satuan,
						ff.id_kategori,
						ff.kategori,
                        dd.to_company_id,
						dd.to_cabang_id,
						hh.company_name as to_company_name,
						ii.cabang_name as to_cabang_name,
                        dd.id_trans as id_trans_inventory_transfer,
                        ee.ppn,
                        ee.pph22
                    FROM trans_inventory_subsidiary_receipt_transfer aa
                    LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                    LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang AND aa.company_id = cc.id_company
					LEFT JOIN trans_inventory_holding_transfer dd ON aa.id_trans_holding_transfer = dd.id_trans
					LEFT JOIN master_produk ee ON dd.produk_id = ee.id_produk
					LEFT JOIN master_produk_kategori ff ON ee.kategori_produk = ff.id_kategori
					LEFT JOIN master_produk_uom_satuan gg ON ee.uom_satuan = gg.id_uom_satuan
					LEFT JOIN master_company hh ON dd.to_company_id = hh.id_company 
					LEFT JOIN master_company_cabang ii ON dd.to_cabang_id = ii.id_cabang AND dd.to_company_id = ii.id_company
                    ) zz"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) count FROM (
                    SELECT 
                        aa.id_trans,
                        bb.id_company as company_id,
                        bb.company_name,
                        cc.id_cabang as cabang_id,
                        cc.cabang_name,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                        ELSE '-'
                        END) as ket_status_release,
                        aa.status_release,
                        aa.tanggal,
                        aa.no_urut,
                        aa.updateindb,
						ee.id_produk as produk_id,
					    ee.nama_produk ||' ('||gg.uom_satuan||')' as nama_produk,
						dd.qty,
						dd.harga_satuan,
						dd.harga_total,
                        dd.transport_cost_total,
						dd.tanggal as tanggal_transfer,
						gg.id_uom_satuan,
						gg.uom_satuan,
						ff.id_kategori,
						ff.kategori,
                        dd.to_company_id,
						dd.to_cabang_id,
						hh.company_name as to_company_name,
						ii.cabang_name as to_cabang_name,
                        dd.id_trans as id_trans_inventory_transfer,
                        ee.ppn,
                        ee.pph22
                    FROM trans_inventory_subsidiary_receipt_transfer aa
                    LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                    LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang AND aa.company_id = cc.id_company
					LEFT JOIN trans_inventory_holding_transfer dd ON aa.id_trans_holding_transfer = dd.id_trans
					LEFT JOIN master_produk ee ON dd.produk_id = ee.id_produk
					LEFT JOIN master_produk_kategori ff ON ee.kategori_produk = ff.id_kategori
					LEFT JOIN master_produk_uom_satuan gg ON ee.uom_satuan = gg.id_uom_satuan
					LEFT JOIN master_company hh ON dd.to_company_id = hh.id_company 
					LEFT JOIN master_company_cabang ii ON dd.to_cabang_id = ii.id_cabang AND dd.to_company_id = ii.id_company
                ) zz """
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

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
            filter_other = f" zz.company_id = '{company_id}' AND zz.cabang_id = '{cabang_id}' AND zz.status_release = true"
            filter_other_conj = f" and "

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
                        bb.id_company as company_id,
                        bb.company_name,
                        cc.id_cabang as cabang_id,
                        cc.cabang_name,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                        ELSE 'draft'
                        END) as ket_status_release,
                        aa.status_release,
                        aa.tanggal,
                        aa.no_urut,
                        aa.updateindb,
						ee.id_produk as produk_id,
					    ee.nama_produk ||' ('||gg.uom_satuan||')' as nama_produk,
						dd.qty,
						dd.harga_satuan,
						dd.harga_total,
                        dd.transport_cost_total,
						dd.tanggal as tanggal_transfer,
					    gg.id_uom_satuan,
						gg.uom_satuan,
						ff.id_kategori,
						ff.kategori,
                        dd.to_company_id,
						dd.to_cabang_id,
						hh.company_name as to_company_name,
						ii.cabang_name as to_cabang_name,
                        dd.id_trans as id_trans_inventory_transfer
                    FROM trans_inventory_subsidiary_receipt_transfer aa
                    LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                    LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang AND aa.company_id = cc.id_company
					LEFT JOIN trans_inventory_holding_transfer dd ON aa.id_trans_holding_transfer = dd.id_trans
					LEFT JOIN master_produk ee ON dd.produk_id = ee.id_produk
					LEFT JOIN master_produk_kategori ff ON ee.kategori_produk = ff.id_kategori
					LEFT JOIN master_produk_uom_satuan gg ON ee.uom_satuan = gg.id_uom_satuan
					LEFT JOIN master_company hh ON dd.to_company_id = hh.id_company 
					LEFT JOIN master_company_cabang ii ON dd.to_cabang_id = ii.id_cabang AND dd.to_company_id = ii.id_company
                    ) zz"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) count FROM (
                    SELECT 
                        aa.id_trans,
                        bb.id_company as company_id,
                        bb.company_name,
                        cc.id_cabang as cabang_id,
                        cc.cabang_name,
                        (CASE 
                            WHEN aa.status_release = true
                            THEN 'release'
                        ELSE 'draft'
                        END) as ket_status_release,
                        aa.status_release,
                        aa.tanggal,
                        aa.no_urut,
                        aa.updateindb,
						ee.id_produk as produk_id,
					    ee.nama_produk ||' ('||gg.uom_satuan||')' as nama_produk,
						dd.qty,
						dd.harga_satuan,
						dd.harga_total,
                        dd.transport_cost_total,
						dd.tanggal as tanggal_transfer,
					    gg.id_uom_satuan,
						gg.uom_satuan,
						ff.id_kategori,
						ff.kategori,
                        dd.to_company_id,
						dd.to_cabang_id,
						hh.company_name as to_company_name,
						ii.cabang_name as to_cabang_name,
                        dd.id_trans as id_trans_inventory_transfer,
                        ee.ppn,
                        ee.pph22
                    FROM trans_inventory_subsidiary_receipt_transfer aa
                    LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                    LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang AND aa.company_id = cc.id_company
					LEFT JOIN trans_inventory_holding_transfer dd ON aa.id_trans_holding_transfer = dd.id_trans
					LEFT JOIN master_produk ee ON dd.produk_id = ee.id_produk
					LEFT JOIN master_produk_kategori ff ON ee.kategori_produk = ff.id_kategori
					LEFT JOIN master_produk_uom_satuan gg ON ee.uom_satuan = gg.id_uom_satuan
					LEFT JOIN master_company hh ON dd.to_company_id = hh.id_company 
					LEFT JOIN master_company_cabang ii ON dd.to_cabang_id = ii.id_cabang AND dd.to_company_id = ii.id_company
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
                        FROM trans_inventory_subsidiary_receipt_transfer 
                        WHERE company_id = {company_id} AND DATE_PART('year', tanggal) = {tahun} AND DATE_PART('month', tanggal) = {bulan}"""
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

    async def release(self, data):
        data_where_update = data["data_where_update"]

        try:
            sql_inv_receipt_in = f"""
                         SELECT 
                            aa.id_trans,
                            aa.company_id,
                            aa.cabang_id,
                            aa.updateindb,
                            aa.userupdate,
                            aa.status_release,
                            aa.tanggal,
                            aa.id_trans_holding_transfer,
                            aa.no_urut,
                            bb.produk_id,
                            bb.qty,
                            bb.harga_satuan,
                            bb.harga_total
                        FROM trans_inventory_subsidiary_receipt_transfer aa
                        LEFT JOIN trans_inventory_holding_transfer bb
						ON aa.id_trans_holding_transfer = bb.id_trans
                        WHERE aa.id_trans = '{data_where_update['id_trans']}'"""

            sql_inv_receipt_out = f"""
                        SELECT 
                            aa.id_trans,
                            bb.company_id,
                            bb.cabang_id,
                            aa.updateindb,
                            aa.userupdate,
                            aa.status_release,
                            aa.tanggal,
                            aa.id_trans_holding_transfer,
                            aa.no_urut,
                            bb.produk_id,
                            bb.qty,
                            ROUND((bb.harga_total+bb.transport_cost_total)/bb.qty,0) as harga_satuan,
                            bb.harga_total+bb.transport_cost_total as harga_total
                        FROM trans_inventory_subsidiary_receipt_transfer aa
                        LEFT JOIN trans_inventory_holding_transfer bb
						ON aa.id_trans_holding_transfer = bb.id_trans
                        WHERE aa.id_trans = '{data_where_update['id_trans']}'"""

            result_inv_receipt = {
                "inv_receipt_in": await self.db.executeToDict(sql_inv_receipt_in),
                "inv_receipt_out": await self.db.executeToDict(sql_inv_receipt_out),
            }

            data_inv_mutasi_in = {
                "produk_id": result_inv_receipt["inv_receipt_in"][0]["produk_id"],
                "company_id": result_inv_receipt["inv_receipt_in"][0][
                    "company_id"
                ],  # --> untuk filter delete dan insert
                "cabang_id": result_inv_receipt["inv_receipt_in"][0]["cabang_id"],
                "qty": result_inv_receipt["inv_receipt_in"][0]["qty"],
                "harga_satuan": result_inv_receipt["inv_receipt_out"][0][
                    "harga_satuan"
                ],
                "harga_total": result_inv_receipt["inv_receipt_out"][0]["harga_total"],
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
                "in_out": "IN",
                "mutasi_type": "TP",
                "id_references": result_inv_receipt["inv_receipt_in"][0]["id_trans"],
                "tabel_reference": "trans_inventory_subsidiary_receipt_transfer",
                "tanggal": result_inv_receipt["inv_receipt_in"][0]["tanggal"],
            }

            data_inv_mutasi_out = {
                "produk_id": result_inv_receipt["inv_receipt_out"][0]["produk_id"],
                "company_id": result_inv_receipt["inv_receipt_out"][0]["company_id"],
                "cabang_id": result_inv_receipt["inv_receipt_out"][0]["cabang_id"],
                "qty": int(result_inv_receipt["inv_receipt_out"][0]["qty"]),
                "harga_satuan": int(
                    result_inv_receipt["inv_receipt_in"][0]["harga_satuan"]
                ),
                "harga_total": int(
                    result_inv_receipt["inv_receipt_in"][0]["harga_total"]
                ),
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
                "in_out": "OUT",
                "mutasi_type": "TP",
                "id_references": result_inv_receipt["inv_receipt_out"][0]["id_trans"],
                "tabel_reference": "trans_inventory_subsidiary_receipt_transfer",
                "tanggal": result_inv_receipt["inv_receipt_out"][0]["tanggal"],
            }

            # --------------------------- insert ke mutasi --------------------------------------------------

            sql_insert_inv_mutasi_in = self.db.genStrInsertSingleObject(
                data_inv_mutasi_in, "trans_inventory_detail_mutasi"
            )
            # await self.db.executeQuery(sql_insert_inv_mutasi_in)

            sql_insert_inv_mutasi_out = self.db.genStrInsertSingleObject(
                data_inv_mutasi_out, "trans_inventory_detail_mutasi"
            )
            # await self.db.executeQuery(sql_insert_inv_mutasi_out)

        except Exception as e:

            print(str(e))
            raise HTTPException(400, ("error insert mutasi IN dan OUT: ", str(e)))

        try:

            sql_detail_mutasi_in = f"""SELECT produk_id,
                                        company_id,
                                        cabang_id,
                                        qty,
                                        CASE 
                                            WHEN harga_total = 0 and qty = 0
                                            THEN 0 
                                            ELSE ROUND(harga_total/qty,0)
                                        END as harga_satuan,
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
                                    WHERE company_id = {result_inv_receipt["inv_receipt_in"][0]["company_id"]} and cabang_id = {result_inv_receipt["inv_receipt_in"][0]["cabang_id"]} and produk_id = {result_inv_receipt["inv_receipt_in"][0]["produk_id"]} 
                                        GROUP BY
                                        produk_id,
                                        company_id,
                                        cabang_id
                                        ) aa"""

            sql_detail_mutasi_out = f"""SELECT produk_id,
                                        company_id,
                                        cabang_id,
                                        qty,
                                        CASE 
                                            WHEN harga_total = 0 and qty = 0
                                            THEN 0 
                                            ELSE ROUND(harga_total/qty,0)
                                        END as harga_satuan,
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
                                    WHERE company_id = {result_inv_receipt["inv_receipt_out"][0]["company_id"]} and cabang_id = {result_inv_receipt["inv_receipt_out"][0]["cabang_id"]} and produk_id = {result_inv_receipt["inv_receipt_out"][0]["produk_id"]} 
                                        GROUP BY
                                        produk_id,
                                        company_id,
                                        cabang_id
                                        ) aa"""

            trans = await self.db.executeTrans(
                [sql_insert_inv_mutasi_in, sql_insert_inv_mutasi_out]
            )
            if trans["status"] == False:
                message = {"status": False, "msg": "Eror. Cek query."}
                raise HTTPException(400, str(trans["detail"]))

            result_detail_mutasi = {
                "detail_mutasi_in": await self.db.executeToDict(sql_detail_mutasi_in),
                "detail_mutasi_out": await self.db.executeToDict(sql_detail_mutasi_out),
            }

            # update status_release = 'true'
            datetime_ = datetime.today()
            sql_update_status_release_inv_receipt = f"""UPDATE trans_inventory_subsidiary_receipt_transfer SET status_release = 'true', tanggal_receipt = '{datetime_}'
            WHERE id_trans = '{data_where_update['id_trans']}'"""
            # await self.db.executeQuery(sql_update_status_release_inv_receipt)

            sql_delete_inv_detail_in = f"""DELETE FROM trans_inventory_detail 
                                    WHERE company_id = {result_inv_receipt["inv_receipt_in"][0]["company_id"]} AND cabang_id = {result_inv_receipt["inv_receipt_in"][0]["cabang_id"]} AND produk_id = {result_inv_receipt["inv_receipt_in"][0]["produk_id"]}"""
            # await self.db.executeQuery(sql_delete_inv_detail_in)

            sql_delete_inv_detail_out = f"""DELETE FROM trans_inventory_detail 
                                    WHERE company_id = {result_inv_receipt["inv_receipt_out"][0]["company_id"]} AND cabang_id = {result_inv_receipt["inv_receipt_out"][0]["cabang_id"]} AND produk_id = {result_inv_receipt["inv_receipt_out"][0]["produk_id"]}"""
            # await self.db.executeQuery(sql_delete_inv_detail_out)

            data_inv_detail_in = {
                "produk_id": result_detail_mutasi["detail_mutasi_in"][0]["produk_id"],
                "company_id": result_detail_mutasi["detail_mutasi_in"][0]["company_id"],
                "cabang_id": result_detail_mutasi["detail_mutasi_in"][0]["cabang_id"],
                "qty": int(result_detail_mutasi["detail_mutasi_in"][0]["qty"]),
                "harga_satuan": int(
                    result_detail_mutasi["detail_mutasi_in"][0]["harga_satuan"]
                ),
                "harga_total": int(
                    result_detail_mutasi["detail_mutasi_in"][0]["harga_total"]
                ),
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
            }

            data_inv_detail_out = {
                "produk_id": result_detail_mutasi["detail_mutasi_out"][0]["produk_id"],
                "company_id": result_detail_mutasi["detail_mutasi_out"][0][
                    "company_id"
                ],
                "cabang_id": result_detail_mutasi["detail_mutasi_out"][0]["cabang_id"],
                "qty": int(result_detail_mutasi["detail_mutasi_out"][0]["qty"]),
                "harga_satuan": int(
                    result_detail_mutasi["detail_mutasi_out"][0]["harga_satuan"]
                ),
                "harga_total": int(
                    result_detail_mutasi["detail_mutasi_out"][0]["harga_total"]
                ),
                "updateindb": datetime.today(),
                "userupdate": auth.AuthAction.get_data_params("username"),
            }

            sql_insert_inv_detail_in = self.db.genStrInsertSingleObject(
                data_inv_detail_in, "trans_inventory_detail"
            )
            # await self.db.executeQuery(sql_insert_inv_detail_in)

            sql_insert_inv_detail_out = self.db.genStrInsertSingleObject(
                data_inv_detail_out, "trans_inventory_detail"
            )
            # await self.db.executeQuery(sql_insert_inv_detail_out)

            # eksekusi all transaksi insert, update, delete
            await self.db.executeTrans(
                [
                    sql_update_status_release_inv_receipt,
                    sql_delete_inv_detail_in,
                    sql_delete_inv_detail_out,
                    sql_insert_inv_detail_in,
                    sql_insert_inv_detail_out,
                ]
            )

        except Exception as e:
            where_del_in = {
                "id_trans": result_detail_mutasi["detail_mutasi_in"][0]["id_trans"],
            }

            where_del_out = {
                "id_trans": result_detail_mutasi["detail_mutasi_out"][0]["id_trans"],
            }

            sql_del_mutasi_in = await self.db.genDeleteObject(
                where_del_in, "trans_inventory_detail_mutasi"
            )
            sql_del_mutasi_out = await self.db.genDeleteObject(
                where_del_out, "trans_inventory_detail_mutasi"
            )

            try:
                await self.db.executeTrans([sql_del_mutasi_in, sql_del_mutasi_out])
            except Exception as e2:
                print(str(e2))
                raise HTTPException(400, ("error ketika delete di mutasi: ", str(e2)))

            message = {"status": False, "msg": "Eror. Cek query."}
            print(str(e))
            raise HTTPException(400, ("The error is: ", str(e)))

        message = {"status": True, "msg": "success"}

        return message

    async def reject(self, data):
        try:
            data_where = data["data_where"]

            # update status_release = 'true'
            sql_update_status_release_inv_receipt = f"""UPDATE trans_inventory_holding_transfer SET status_release = 'false'
            WHERE id_trans = (SELECT id_trans_holding_transfer FROM trans_inventory_subsidiary_receipt_transfer WHERE id_trans = '{data_where['id_trans']}')"""
            await self.db.executeQuery(sql_update_status_release_inv_receipt)

            sql_delete_inv_receipt_transfer = f"""DELETE FROM trans_inventory_subsidiary_receipt_transfer 
                                    WHERE id_trans = '{data_where['id_trans']}'"""
            await self.db.executeQuery(sql_delete_inv_receipt_transfer)

            message = {"status": True, "msg": "success"}
        except Exception as e:
            message = {"status": False, "msg": "Eror. Cek query."}
        return message


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_subsidiary_inventory_receipt_transfer/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_trans/c_subsidiary_inventory_receipt_transfer/read_pending")
async def read_pending(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
):
    ob_data = c_subsidiary_inventory_receipt_transfer()
    return await ob_data.read_pending(
        orderby, limit, offset, filter, company_id, cabang_id
    )


@app.get("/api/f_trans/c_subsidiary_inventory_receipt_transfer/read_history")
async def read_history(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="$company_id"),
    cabang_id: int = Query(None, alias="$cabang_id"),
):
    ob_data = c_subsidiary_inventory_receipt_transfer()
    return await ob_data.read_history(
        orderby, limit, offset, filter, company_id, cabang_id
    )


@app.post("/api/f_trans/c_subsidiary_inventory_receipt_transfer/release")
async def release(request: Request):
    data = await request.json()
    ob_data = c_subsidiary_inventory_receipt_transfer()
    return await ob_data.release(data)


@app.get("/api/f_trans/c_subsidiary_inventory_receipt_transfer/get_id_trans_kode")
async def get_id_trans_kode(company_id, cabang_id, kode_trans, tahun, bulan):
    ob_data = c_subsidiary_inventory_receipt_transfer()
    return await ob_data.get_id_trans_kode(
        company_id, cabang_id, kode_trans, tahun, bulan
    )


@app.get("/api/f_trans/c_subsidiary_inventory_receipt_transfer/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_subsidiary_inventory_receipt_transfer()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_trans/c_subsidiary_inventory_receipt_transfer/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_subsidiary_inventory_receipt_transfer()
    path_parent = params.loc["file_inventory_receipt_transfer"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)
