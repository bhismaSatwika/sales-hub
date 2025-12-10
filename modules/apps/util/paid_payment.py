from fastapi import HTTPException
from library.db import Db
from modules.apps.util.convert_value import convert_value


class paid_payment(object):
    def __init__(self):
        self.db = Db()

    async def validasi_paid_payment(self, data):
        sql = f"""
        SELECT X.*, amount_total_outstanding_inv + amount_total_outstanding_pre as total FROM (
            SELECT
            b.id_customer,nama_customer,
            CASE 
            WHEN a.amount_total_outstanding is NULL THEN
                0
            ELSE
                a.amount_total_outstanding
            END as amount_total_outstanding_inv,
            CASE 
            WHEN c.amount_total_outstanding is NULL THEN
                0
            ELSE
                c.amount_total_outstanding
            END as amount_total_outstanding_pre
            FROM
            master_customer B
            LEFT JOIN (
            SELECT customer_id, sum(amount_total_outstanding) as amount_total_outstanding from trans_inventory_subsidiary_invoice
            WHERE customer_id = {data['customer_id']}
            GROUP BY customer_id 
            ) A on A.customer_id = B.id_customer
            LEFT JOIN (
            SELECT sum(A.amount_total_outstanding) as amount_total_outstanding, A.customer_id FROM trans_inventory_subsidiary_invoice_pre_payment A
            WHERE A.customer_id = {data['customer_id']}
            GROUP BY A.customer_id
            ) C on C.customer_id = B.id_customer
            WHERE b.id_customer = {data['customer_id']}
        ) X
        """
        print("query payment", sql)

        message = ""
        try:
            result = await self.db.executeToDict(sql)
            result_data = result[0]
            sum = result_data["total"]

            if sum == 0:
                return "Success"

            else:
                value = convert_value(sum)
                customer_name = result_data["nama_customer"]
                message = f"Ada payment pada customer {customer_name} yang belum lunas sebesar Rp. {value}"
                raise HTTPException(
                    status_code=400,
                    detail=message,
                )

        except Exception as e:
            print(message + str(e))
            message = "Gagal ketika rilis sales order: " + message + str(e)
            raise HTTPException(
                status_code=400,
                detail=message,
            )
