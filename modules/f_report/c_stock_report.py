import base64
import io

from fastapi.responses import StreamingResponse

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font


class c_stock_report(object):
    def __init__(self):
        self.db = Db()

    async def generate_report(self):
        query = f"""
            SELECT 
        C.company_name,
        D.cabang_name,
        E.nama_produk,
        A.qty_tf,
        A.current,
        (A.qty_tf -
        A.current) SOLD
        FROM
        (
            SELECT 
            A.to_company_id,
            A.to_cabang_id,
            A.produk_id,
            SUM ( A.qty ) AS qty_tf,
            SUM ( B.qty ) AS CURRENT 
            FROM
            trans_inventory_holding_transfer
            A LEFT JOIN trans_inventory_detail B ON A.produk_id = B.produk_id 
            AND A.to_company_id = B.company_id 
            AND A.to_cabang_id = B.cabang_id 
            GROUP BY
            A.to_company_id,
            A.to_cabang_id,
            A.produk_id
        )
        A LEFT JOIN master_company C ON A.to_company_id = C.id_company
        LEFT JOIN master_company_cabang D ON A.to_company_id = D.id_company AND A.to_cabang_id = D.id_cabang
        LEFT JOIN master_produk E on A.produk_id = E.id_produk 
        """

        res = await self.db.executeToDict(query)
        wb = self.generate_excel(res)
        buffer = io.BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=example.xlsx"},
        )

    def generate_excel(self, result_data):
        wb = Workbook()
        ws = wb.active
        ws["A1"].value = "Nama Company"
        ws["B1"].value = "Nama Cabang"
        ws["C1"].value = "Nama Produk"
        ws["D1"].value = "Quantity Transfer"
        ws["E1"].value = "Quantity Sekarang"
        ws["F1"].value = "Quantity Terjual"

        if len(result_data) > 0:
            data_key = []
            i = 0

        x = 0
        for key, value in result_data[0].items():
            data_key.append(key)
            ws.cell(1, x + 1).font = Font(b=True, color="000000")
            ws.cell(1, x + 1).fill = PatternFill(
                start_color="ffff00", end_color="ffff00", fill_type="solid"
            )
            x = x + 1

        for data in result_data:
            data_export = []
            for key in data_key:
                data_export.append(data[key])
            ws.append(data_export)
            i = i + 1

        return wb


@app.get("/api/f_report/c_stock_report/get_stock_report")
async def test_get():
    ob_data = c_stock_report()
    return await ob_data.generate_report()
