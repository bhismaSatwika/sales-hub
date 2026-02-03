from fastapi import HTTPException
from library.db import Db
import asyncio
from modules.f_trans.delivery_order_create_pdf import PDF as PDF_DO
from library.router import app


class GenerateDO:
    def __init__(self):
        self.db = Db()

    async def get_all_released_so(self):
        sql = """
          SELECT A.id_trans FROM trans_inventory_subsidiary_sales_order_header A
            LEFT JOIN trans_inventory_subsidiary_invoice B
              ON A.id_trans = B.id_trans_sales_order
            WHERE b.id_trans = 'IDFOOD.NUS.45.INV.2026.01.0050'
        """

        result = await self.db.executeToDict(sql)
        # print(result)
        try:
            for data in result:
                await self.create_pdf_do(data["id_trans"])
                print(data)
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))

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
        PDF_DO(data_header, data_detail)

        pdf = PDF_DO(data_header, data_detail)

        pdf.generate_report()
        # filenamex = data_header["id_trans"]

        # return StreamingResponse(
        #     pdf_buffer,
        #     media_type="application/pdf",
        #     headers={"Content-Disposition": f"inline; filename={filenamex}.pdf"},
        # )


@app.get("/generete_do_fix")
async def release():

    ob_data = GenerateDO()

    return await ob_data.get_all_released_so()


# async def main():
#     gen = GenerateDO()
#     data = await gen.get_all_released_so()
#     # print(data)


# if __name__ == "__main__":
#     asyncio.run(main())
