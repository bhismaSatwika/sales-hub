import base64

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_sales_report(object):
    def __init__(self):
        self.db = Db()

    async def sales_report(self, company_id, cabang_id, produk_id, tanggal):
        return 0


@app.get("/api/f_report/c_sales_report/testing")
async def get_sales_report(
    company_id: int = Query(None, alias="company_id"),
    cabang_id: int = Query(None, alias="cabang_id"),
    produk_id: int = Query(None, alias="produk_id"),
    tanggal: str = Query(None, alias="tanggal"),
):
    ob_data = c_sales_report()
    return await ob_data.sales_report(company_id, cabang_id, produk_id, tanggal)
