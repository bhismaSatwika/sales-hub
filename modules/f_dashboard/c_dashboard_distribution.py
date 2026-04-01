import base64
import json
from typing import Optional

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_dashboard_distribution(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def dropdown(self):
        sql = """
            SELECT id_kategori as value, kategori as text, CASE 
            WHEN id_kategori = 4 THEN
                true
            ELSE
                false
        END as isdefault
        FROM master_produk_kategori
        """
        return {"commodity": await self.db.executeToDict(sql)}

    async def get_data_card(self, param):
        data = json.loads(param)

        where_company = (
            f"""and company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_date = f"""
            WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'
        """

        sql_ = f"""
            SELECT B.kategori as product_name, B.uom_base as uom, COALESCE(A.mtd,0) as mtd, COALESCE(A.ytd,0) as ytd FROM 
            master_produk_kategori B 
            LEFT JOIN 
            (
                SELECT C.kategori_produk, SUM(A.qty * D.uom_base_convert) as mtd, SUM(B.qty * D.uom_base_convert) as ytd FROM (
                SELECT B.produk_id, SUM(B.qty) as qty FROM trans_inventory_subsidiary_invoice A
                LEFT JOIN trans_inventory_subsidiary_sales_order B on A.id_trans_sales_order = B.id_trans
                WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}' {where_company}
                GROUP BY b.produk_id
                ) A
                LEFT JOIN (
                SELECT B.produk_id, SUM(B.qty) as qty FROM trans_inventory_subsidiary_invoice A
                LEFT JOIN trans_inventory_subsidiary_sales_order B on A.id_trans_sales_order = B.id_trans
                WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}' {where_company}
                GROUP BY b.produk_id
                ) B on A.produk_id = B.produk_id
                LEFT JOIN master_produk C on A.produk_id = C.id_produk
                LEFT JOIN master_produk_uom_satuan D on C.uom_satuan = D.id_uom_satuan
                GROUP BY C.kategori_produk
            )A on A.kategori_produk = B.id_kategori
            ORDER BY b.id_kategori desc
                    """

        print(sql_)
        res = await self.db.executeToDict(sql_)

        result = {"data": res}
        return result

    async def get_data_map(self, param):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        sql_ = f"""
            SELECT b.nama, b.long::NUMERIC as lng, b.lat::NUMERIC, qty as value FROM (
            SELECT D.kode_prov, SUM(c.qty * uom_base_convert) as qty  FROM trans_inventory_subsidiary_invoice A
            LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
            LEFT JOIN trans_inventory_subsidiary_sales_order C on C.id_trans = B.id_trans
            LEFT JOIN master_customer D on B.customer_id = D.id_customer
            LEFT JOIN master_produk E on C.produk_id = E.id_produk
                LEFT JOIN master_produk_uom_satuan F on E.uom_satuan = F.id_uom_satuan
            {where_date}
            {where_company} {where_category}
            GROUP BY d.kode_prov
            ) A
            LEFT JOIN  master_provinsi B on A.kode_prov = B.kode_prov
        """

        print(sql_)
        return await self.db.executeToDict(sql_)

    async def get_data_table_1(
        self,
        top,
        orderby,
        skip,
        filter,
        param,
        filter_other="",
        filter_other_conj="",
    ):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        str_clause = self.kendoParse().parse_query(
            orderby, top, skip, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = f"""
            SELECT  b.nama as province, qty as quantity, c.target, c.real FROM (
            SELECT D.kode_prov, SUM(c.qty * uom_base_convert) as qty  FROM trans_inventory_subsidiary_invoice A
            LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
            LEFT JOIN trans_inventory_subsidiary_sales_order C on C.id_trans = B.id_trans
            LEFT JOIN master_customer D on B.customer_id = D.id_customer
            LEFT JOIN master_produk E on C.produk_id = E.id_produk
                LEFT JOIN master_produk_uom_satuan F on E.uom_satuan = F.id_uom_satuan
            {where_date}
            {where_company} {where_category}
            GROUP BY d.kode_prov
            ) A
            LEFT JOIN  master_provinsi B on A.kode_prov = B.kode_prov
            LEFT JOIN data_pasar_pantuan C on A.kode_prov = C.id_prov
        """

        sql = query + str_clause
        sql_2 = query + str_clause_count
        print(sql)

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""
        print(sql_count)

        data = {
            "data": await self.db.executeToDict(sql),
            "total": (await self.db.executeToDict(sql_count))[0]["count"],
        }
        return data

    async def get_data_bar_chart(self, param):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        sql_ = f"""
            SELECT A.*, B.color as warna, 0 as precentage FROM (
            SELECT D.unit_geografis as kategori, SUM(c.qty) as nilai  FROM trans_inventory_subsidiary_invoice A
            LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
            LEFT JOIN trans_inventory_subsidiary_sales_order C on C.id_trans = B.id_trans
            LEFT JOIN master_company_cabang D on B.company_id = D.id_company AND B.cabang_id = D.id_cabang
            LEFT JOIN master_produk E on C.produk_id = E.id_produk
            {where_date} 
            {where_company} {where_category}
            GROUP BY d.unit_geografis
            ) A
            LEFT JOIN master_unit_geografis B on B.unit_geografis = A.kategori
        """

        return await self.db.executeToDict(sql_)

    async def get_data_bar_chart_3(self, param):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        sql_ = f"""
            SELECT e.unit_geografis as kategori, SUM(c.qty * uom_base_convert) as realisasi, SUM(c.qty) as target, 100 as precentage FROM trans_inventory_subsidiary_invoice A
            LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
            LEFT JOIN trans_inventory_subsidiary_sales_order C on C.id_trans = B.id_trans
            LEFT JOIN master_customer D on B.customer_id = D.id_customer
            LEFT JOIN master_provinsi E on D.kode_prov = E.kode_prov
            LEFT JOIN master_produk F on C.produk_id = F.id_produk
            LEFT JOIN master_produk_uom_satuan G on F.uom_satuan = G.id_uom_satuan
            {where_date} 
            {where_company} {where_category}
            GROUP BY e.unit_geografis;
        """
        print(sql_)

        return await self.db.executeToDict(sql_)

    async def get_data_line_chart(self, param):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        sql_ = f"""
            SELECT
            EXTRACT(EPOCH FROM tanggal_invoice::timestamp without time zone)*1000 as tanggal,
            SUM ( c.qty ) as nilai  
            FROM trans_inventory_subsidiary_invoice A
            LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
            LEFT JOIN trans_inventory_subsidiary_sales_order C on B.id_trans = C.id_trans
            LEFT JOIN master_produk D on C.produk_id = D.id_produk
            {where_date} 
            {where_company} {where_category}
            GROUP BY tanggal_invoice, D.kategori_produk
            ORDER By tanggal_invoice
        """
        data_line = []
        for item in await self.db.executeToDict(sql_):
            data_line.append([item["tanggal"], item["nilai"]])

        return data_line

    async def get_data_bar_chart_2(self, param):
        data = json.loads(param)

        where_company = (
            f"""and b.company_id = {data["whereCompany"]}"""
            if data["whereCompany"] != 0
            else ""
        )

        where_category = f"""and kategori_produk = {data["whereCommodity"]}"""

        where_date = f"""WHERE date_part('month', tanggal_invoice) <= EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""
        if data["flag"] == True:
            where_date = f"""WHERE date_part('month', tanggal_invoice) = EXTRACT ( MONTH FROM '{data["whereDate"]}'::date ) AND date_part('year', tanggal_invoice) = EXTRACT(YEAR FROM '{data["whereDate"]}'::DATE) and tanggal_invoice <= '{data["whereDate"]}'"""

        year = data["whereDate"].split("-")[0]
        sql_ = f"""
            SELECT TRIM(to_char(to_date(a.month_::text, 'MM'), 'Month')) AS bulan, b.nilai, b.nilai as target FROM (
                        SELECT
                            month_,
                            year_ 
                        FROM
                            (
                            SELECT
                                date_part( 'month', date_ ) AS month_,
                                date_part( 'year', date_ ) AS year_ 
                            FROM
                                ( SELECT generate_series ( '{year}-01-01' :: DATE, '{year}-12-31' :: DATE, '1 day' :: INTERVAL ) AS date_ ) AS dates 
                            ) A 
                        GROUP BY
                            month_,
                            year_ 
                        ORDER BY
                            year_,
                            month_
                            ) A
                            LEFT JOIN (
                              SELECT date_part('month', tanggal_invoice) as month_, date_part('year', tanggal_invoice) as year_ ,  SUM(c.qty * uom_base_convert) as nilai  FROM trans_inventory_subsidiary_invoice A
                              LEFT JOIN trans_inventory_subsidiary_sales_order_header B on A.id_trans_sales_order = B.id_trans
                              LEFT JOIN trans_inventory_subsidiary_sales_order C on C.id_trans = B.id_trans
                              LEFT JOIN master_produk D on C.produk_id = D.id_produk
                              
            LEFT JOIN master_produk_uom_satuan G on D.uom_satuan = G.id_uom_satuan
                              {where_date} {where_company} {where_category}
                              GROUP BY date_part('month', tanggal_invoice), date_part('year', tanggal_invoice)
                            )  B on A.month_ = B.month_ AND A.year_ = B.year_
                        
        """
        return await self.db.executeToDict(sql_)

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        company_id=None,
        product_id=None,
        filter_other="",
        filter_other_conj="",
    ):

        where = f"""
            WHERE d.id_kategori = '{product_id}'
        """

        if company_id:
            where = f"""
                WHERE d.id_kategori = '{product_id}' AND a.company_id = '{company_id}'
            """

        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        # print(str_clause)

        query = f"""SELECT * FROM (
        SELECT E.company_name, F.cabang_name, SUM(A.qty * C.uom_base_convert) as qty_convert, D.kategori as nama_produk FROM trans_inventory_detail A
        LEFT JOIN master_produk B on A.produk_id = B.id_produk
        LEFT JOIN master_produk_uom_satuan C on B.uom_satuan = C.id_uom_satuan
        LEFT JOIN master_produk_kategori D on B.kategori_produk = D.id_kategori
        LEFT JOIN master_company E on A.company_id = E.id_company
        LEFT JOIN master_company_cabang F on A.cabang_id = F.id_cabang AND A.company_id = F.id_company
        {where}
        GROUP BY E.company_name, F.cabang_name, D.kategori) X
        """

        sql = query + str_clause
        sql_2 = query + str_clause_count
        # print(sql)
        print(sql)

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""
        print(sql_count)

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def read_product(
        self,
        orderby,
        limit,
        offset,
        filter,
        company_id=None,
        product_id=None,
        filter_other="",
        filter_other_conj="",
    ):

        where = f"""
            WHERE d.id_kategori = '{product_id}'
        """

        if company_id:
            where = f"""
                WHERE d.id_kategori = '{product_id}' AND a.company_id = '{company_id}'
            """

        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = f"""SELECT * FROM (
        SELECT SUM(A.qty * C.uom_base_convert) as qty_convert, D.kategori as nama_produk FROM trans_inventory_detail A
        LEFT JOIN master_produk B on A.produk_id = B.id_produk
        LEFT JOIN master_produk_uom_satuan C on B.uom_satuan = C.id_uom_satuan
        LEFT JOIN master_produk_kategori D on B.kategori_produk = D.id_kategori
        {where}
        GROUP BY D.kategori ) X
        """
        # print(sql)
        sql = query + str_clause
        sql_2 = query + str_clause_count
        print(sql)

        sql_count = f"""SELECT COUNT(*) 
        FROM ({sql_2})  as subquery"""

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "total": result_count[0]["count"]}
        return data

    async def read_inventory_card(self, company_id=None, product_id=None):

        where = """
        """
        where_company = f"""
            
        """

        where = f"""
            WHERE d.id_kategori = '{product_id}'
        """

        if company_id:
            where = f"""
                WHERE d.id_kategori = '{product_id}' AND a.company_id = '{company_id}'
            """
            where_company = f"""
                WHERE company_id = '{company_id}'
            """

        sql_total_branch = f"""
                 SELECT COUNT(cabang_id) as total_branch FROM (
        SELECT company_id, cabang_id FROM trans_inventory_detail A
        LEFT JOIN master_produk B on A.produk_id = B.id_produk
        LEFT JOIN master_produk_uom_satuan C on B.uom_satuan = C.id_uom_satuan
        LEFT JOIN master_produk_kategori D on B.kategori_produk = D.id_kategori
        {where}
        GROUP BY company_id, cabang_id
        ) X
        """

        sql_total_product = f""" 
        SELECT COUNT(DISTINCT produk_id) as total_product FROM (
        SELECT A.produk_id FROM trans_inventory_detail A
        LEFT JOIN master_produk B on A.produk_id = B.id_produk
        LEFT JOIN master_produk_uom_satuan C on B.uom_satuan = C.id_uom_satuan
        LEFT JOIN master_produk_kategori D on B.kategori_produk = D.id_kategori
        {where}
        ) X
        """

        print(sql_total_branch)
        print(sql_total_product)

        result_total_branch = await self.db.executeToDict(sql_total_branch)
        result_total_product = await self.db.executeToDict(sql_total_product)

        data = {
            "total_branch": result_total_branch[0]["total_branch"],
            "total_product": result_total_product[0]["total_product"],
        }
        return data


@app.get("/api/f_dashboard/c_dashboard_distribution/read")
async def read(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: Optional[int] = Query(None, alias="$company_id"),
    product_id: int = Query(None, alias="$product_id"),
):
    ob_data = c_dashboard_distribution()
    return await ob_data.read(orderby, limit, offset, filter, company_id, product_id)


@app.get("/api/f_dashboard/c_dashboard_distribution/read_product")
async def read_product(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: Optional[int] = Query(None, alias="$company_id"),
    product_id: int = Query(None, alias="$product_id"),
):
    ob_data = c_dashboard_distribution()
    return await ob_data.read_product(
        orderby, limit, offset, filter, company_id, product_id
    )


@app.get("/api/f_dashboard/c_dashboard_distribution/read_inventory_card")
async def read_inventory_card(
    company_id: Optional[int] = Query(None, alias="$company_id"),
    product_id: int = Query(None, alias="$product_id"),
):
    ob_data = c_dashboard_distribution()
    return await ob_data.read_inventory_card(company_id, product_id)


@app.get("/api/f_dashboard/c_dashboard_distribution/dropdown")
async def get_data_card():
    ob_data = c_dashboard_distribution()
    return await ob_data.dropdown()


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_card")
async def get_data_card(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_card(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_map")
async def get_data_map(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_map(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_bar_chart")
async def get_data_bar_chart(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_bar_chart(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_bar_chart_2")
async def get_data_bar_chart_2(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_bar_chart_2(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_bar_chart_3")
async def get_data_bar_chart_3(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_bar_chart_3(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_line_chart")
async def get_data_line_chart(param):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_line_chart(param)


# @app.get("/api/f_dashboard/c_dashboard_distribution/get_data_pie_chart")
# async def get_data_pie_chart(param):
#     ob_data = c_dashboard_distribution()
#     return await ob_data.get_data_pie_chart(param)


@app.get("/api/f_dashboard/c_dashboard_distribution/get_data_table_1")
async def get_data_table_1(top=None, orderby=None, skip=None, filter=None, param=None):
    ob_data = c_dashboard_distribution()
    return await ob_data.get_data_table_1(top, orderby, skip, filter, param)


# @app.get("/api/f_dashboard/c_dashboard_distribution/get_data_table_2")
# async def get_data_table_2(top=None, orderby=None, skip=None, filter=None, param=None):
#     ob_data = c_dashboard_distribution()
#     return await ob_data.get_data_table_2(top, orderby, skip, filter, param)
