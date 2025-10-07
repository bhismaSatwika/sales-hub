import base64
import hashlib
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import HTTPException, Depends
from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel


def replaceForSqlInjection(sqlStr):
    a = ["'", '"']

    for item in a:
        sqlStr = str(sqlStr).replace(item, "")

    return sqlStr


class login(object):
    def __init__(self):
        self.db = Db()

    async def login(self, username):
        isValid = await self._is_valid_user(username)

        if isValid:
            return auth.AuthAction.create_token(self.result[0])
        else:
            raise HTTPException(
                status_code=401,
                detail={"status": -1, "message": "Username Password Wrong"},
            )

    async def _is_valid_user(self, username):
        sql = f"""SELECT aa.id_user,
                         aa.username,
                         bb.role_data,
                         bb.id_role,
                         cc.id_company as company_id,
                         cc.company_name,
                         dd.id_cabang as cabang_id,
                         dd.cabang_name,
                         aa.is_view_only
                FROM master_user aa
                LEFT JOIN master_user_role bb ON aa.user_role = bb.id_role
                LEFT JOIN master_company cc ON aa.company_id = cc.id_company
                LEFT JOIN master_company_cabang dd ON aa.cabang_id = dd.id_cabang
                WHERE aa.username='{username}'"""

        # print(sql)
        self.result = await self.db.executeToDict(sql)
        # print(self.result)
        rowcount = len(self.result)
        if rowcount > 0:
            return True
        else:
            return False


"""
list your path url at bottom
example /testing url
test from postman :
url/api/login/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.post("/api/apps/login")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    ob_data = login()
    data = await ob_data.login(
        replaceForSqlInjection(form_data.username),
        replaceForSqlInjection(form_data.password),
    )
    return data


# @app.post("/api/apps/login")
# async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
#     ob_data = login()
#     data = await ob_data.login(form_data.username, form_data.password)
#     return data
