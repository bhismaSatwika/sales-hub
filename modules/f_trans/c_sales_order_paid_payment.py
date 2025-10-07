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

class c_sales_order_paid_payment(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""):

        if orderby == None or orderby == '':
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
                        aa.company_id,
                        bb.company_name,
                        aa.cabang_id,
                        cc.cabang_name,
                        aa.customer_id,
                        dd.nama_customer,
                        dd.alamat,
                        dd.email,
                        dd.no_hp,
                        dd.no_hp,
                        dd.npwp,
                        aa.virtual_account,
                        aa.nominal,
                        aa.updateindb,
                        aa.userupdate
                FROM trans_sales_order_paid_payment aa
                LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang
                LEFT JOIN master_customer dd ON aa.customer_id = dd.id_customer
                ) zz """
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) count FROM (
                    SELECT 
                        aa.company_id,
                        bb.company_name,
                        aa.cabang_id,
                        cc.cabang_name,
                        aa.customer_id,
                        dd.nama_customer,
                        dd.alamat,
                        dd.email,
                        dd.no_hp,
                        dd.no_hp,
                        dd.npwp,
                        aa.virtual_account,
                        aa.nominal,
                        aa.updateindb,
                        aa.userupdate
                FROM trans_sales_order_paid_payment aa
                LEFT JOIN master_company bb ON aa.company_id = bb.id_company
                LEFT JOIN master_company_cabang cc ON aa.cabang_id = cc.id_cabang
                LEFT JOIN master_customer dd ON aa.customer_id = dd.id_customer
                ) zz """
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data
    

"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_sales_order_paid_payment/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""

