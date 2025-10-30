import base64

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_salesman_report(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        salesman=None,
        company_id=None,
        cabang_id=None,
        filter_other="",
        filter_other_conj="",
    ):

        salesman_filter = f"salesman = '{salesman}'"
        salesman_and = "and"

        if salesman == 0:
            salesman_filter = f""
            salesman_and = ""

        filter_other = f" company_id = '{company_id}' AND cabang_id = '{cabang_id}'{salesman_and} {salesman_filter}"
        filter_other_conj = f" and "

        if company_id == 1:
            filter_other = f"(1=1) {salesman_and}  {salesman_filter}"
            filter_other_conj = f""

        if company_id == 2 and cabang_id == 11:
            filter_other = (
                f" company_id = '{company_id}' {salesman_and} {salesman_filter}"
            )
            filter_other_conj = f" and "

        # print(filter_other)
        if orderby == None or orderby == "":
            orderby = "name"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = (
            f"""
            SELECT
        B.NAME,
        B.company_id,
        B.cabang_id,
        A.*,
        ROUND(total_paid / total_sales * 100,2)::FLOAT as paid_percentage
        FROM
        (
            SELECT
            b.salesman,
            SUM ( A.amount_total ) AS total_sales,
            SUM ( A.qty ) AS qty,
            SUM ( A.amount_total - A.amount_total_outstanding ) AS total_paid 
            FROM
            trans_inventory_subsidiary_invoice A 
            LEFT JOIN trans_inventory_subsidiary_sales_order B ON A.id_trans_sales_order = B.id_trans
            WHERE b.status_release = true
            GROUP BY
            b.salesman 
        )
        A LEFT JOIN master_user B ON A.salesman = B.id_user
        
        """
            + str_clause
        )

        query_count = (
            f"""
           SELECT
        COUNT(*) as count
        FROM
        (
            SELECT
            b.salesman,
            SUM ( A.amount_total ) AS total_sales,
            SUM ( A.qty ) AS qty,
            SUM ( A.amount_total - A.amount_total_outstanding ) AS total_paid 
            FROM
            trans_inventory_subsidiary_invoice A 
            LEFT JOIN trans_inventory_subsidiary_sales_order B ON A.id_trans_sales_order = B.id_trans
            WHERE b.status_release = true
            GROUP BY
            b.salesman 
        )
        A LEFT JOIN master_user B ON A.salesman = B.id_user
          """
            + str_clause_count
        )

        print(query)

        result = await self.db.executeToDict(query)
        result_count = await self.db.executeToDict(query_count)
        data = {"data": result, "total": result_count[0]["count"]}
        return data


@app.get("/api/f_report/c_salesman_report/read")
async def test_get(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    company_id: int = Query(None, alias="company_id"),
    cabang_id: int = Query(None, alias="cabang_id"),
    salesman: int = Query(None, alias="salesman"),
):
    ob_data = c_salesman_report()
    return await ob_data.read(
        orderby, limit, offset, filter, salesman, company_id, cabang_id
    )
