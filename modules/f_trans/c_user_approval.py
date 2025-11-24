import base64

from fastapi import Query

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class c_user_approval(object):
    def __init__(self):
        self.db = Db()

    async def get_approval_progress(self, id_header):
        sql = f"""
            SELECT C.name, D.status_name, A.action_time, A.order_approve, A.description from trans_approval_detail A
        LEFT JOIN master_approval B on A.master_approval_id = B."id"
        LEFT JOIN master_user C on B.username = C.username
        LEFT JOIN master_approval_status D on A.approval_status = D.id_status
                LEFT JOIN trans_inventory_subsidiary_sales_order_header E on A.header_id = E.id_trans
        WHERE A.header_id = '{id_header}' and a.active = true and B.approval_company_id= E.company_id AND B.approval_cabang_id = E.cabang_id
        ORDER BY A.order_approve
        """

        return await self.db.executeToDict(sql)


@app.get("/api/f_trans/c_user_approval/get_approval_progress")
async def get_approval_progress(
    id_header: str = Query(None, alias="id_header"),
):
    ob_data = c_user_approval()
    return await ob_data.get_approval_progress(id_header)
