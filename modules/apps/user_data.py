import base64

from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


class user_data(object):
    def __init__(self):
        self.db = Db()

    async def get_user_data(self, id, uuid):
        sql = f"""
                    SELECT aa.id_user,
                    aa.username,
                    bb.role_data,
                    bb.id_role,
                    cc.id_company as company_id,
                    cc.company_name,
                    dd.id_cabang as cabang_id,
                    dd.cabang_name,
                    aa.is_view_only
                    ,aa.uuid,
                    dd.is_pusat,
                    ee.version_id
            FROM master_user aa
            LEFT JOIN master_user_role bb ON aa.user_role = bb.id_role
            LEFT JOIN master_company cc ON aa.company_id = cc.id_company
            LEFT JOIN master_company_cabang dd ON aa.cabang_id = dd.id_cabang and aa.company_id = dd.id_company
            JOIN version_apps ee on 1= 1 and ee.default_ = true
            WHERE id_user='{id}' and uuid = '{uuid}' 
        """

        result = await self.db.executeToDict(sql)
        return result[0]


@app.get("/api/apps/user_data/get_user_data")
async def get_user_data(
    id: int,
    uuid: str,
):
    ob_data = user_data()
    return await ob_data.get_user_data(id, uuid)
