from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db
from config.db_config import db_config_sso


class c_master_user(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse
        self.db_sso = Db(db_config_sso)

    async def validate_user_sso(self, nik):
        sql = f"""SELECT count(*) count FROM public.tbl_user_access WHERE nik = {nik}"""
        result = await self.db_sso.executeQuery(sql)
        jumlah = result[0]["count"]
        if jumlah != 0 and jumlah != None:
            return True
        else:
            return False

    async def create_user_sso(self, data):
        user_sso = {
            "userid_or_nik": data["username"],
            "email": data["email"],
            "wa_phone_number": data["wa_phone_number"],
            "updateindb": datetime.today(),
        }

        sqlDataSso = self.db.genStrInsertSingleObject(user_sso, "tbl_user_access")

        try:
            await self.db.executeQuery(sqlDataSso)
            message = {"status": "success"}

        except Exception as e:
            message = {"status": "error : " + str(e)}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def create(self, data):
        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlData = self.db.genStrInsertSingleObject(data, "master_user")

        try:

            await self.db.executeQuery(sqlData)
            message = {"status": "success"}

        except Exception as e:
            message = {"status": "error : " + str(e)}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == "":
            orderby = "x.updateindb DESC"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT * FROM (
                    SELECT
                       aa.id_user,
                       aa.username,
                       aa.name,
                       aa.company_id,
                       aa.cabang_id,
                       cc.company_name,
                       dd.cabang_name,
                       aa.updateindb,
                       bb.id_role,
                       bb.role_name,
                       bb.role_data,
                       aa.status_release,
                       aa.status_aktif,
                       (CASE
                        WHEN is_salesman = 't'
                        THEN 'Salesman'
                        ELSE 'Bukan Salesman'
                        END) as status_salesman
                    FROM master_user aa
                    LEFT JOIN master_user_role bb ON aa.user_role = bb.id_role
                    LEFT JOIN master_company cc ON aa.company_id = cc.id_company
                    LEFT JOIN master_company_cabang dd ON aa.cabang_id = dd.id_cabang) x"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) as count FROM 
                        (
                        SELECT
                            aa.id_user,
                            aa.username,
                            aa.company_id,
                            aa.cabang_id,
                            cc.company_name,
                            dd.cabang_name,
                            aa.updateindb,
                            bb.id_role,
                            bb.role_name,
                            bb.role_data,
                            (CASE
                                WHEN is_salesman = 't'
                                THEN 'Salesman'
                                ELSE 'Bukan Salesman'
                            END) as status_salesman
                        FROM master_user aa
                        LEFT JOIN master_user_role bb ON aa.user_role = bb.id_role
                        LEFT JOIN master_company cc ON aa.company_id = cc.id_company
                        LEFT JOIN master_company_cabang dd ON aa.cabang_id = dd.id_cabang
                        ) x"""
            + str_clause_count
        )

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def update(self, data, data_where):

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlData = self.db.genUpdateObject(data, data_where, "master_user")

        # print(sqlData)
        try:
            await self.db.executeQuery(sqlData)
            # await self.db.executeQuery(sqlDataPrivilege)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_user")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_user_salesman(self, where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']} AND status_release = 't' AND status_aktif = 't' AND is_salesman = 't'"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT id_user as value,name as text
				    FROM master_user 
                    {where_sql}
                    ORDER BY id_user ASC"""
        try:
            result = await self.db.executeToDict(sql)
        except Exception as e:
            raise HTTPException(400, ("The error is: ", str(e)))

        return result


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_user/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_setting/c_master_user/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    ob_data = c_master_user()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_setting/c_master_user/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_user()
    return await ob_data.create(data)


@app.post("/api/f_setting/c_master_user/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_user()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_setting/c_master_user/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_user()
    return await ob_data.delete(data)


@app.get("/api/f_setting/c_master_user/validate_user_sso")
async def validate_user_sso(nik):
    ob_data = c_master_user()
    return await ob_data.validate_user_sso(nik)


@app.post("/api/f_setting/c_master_user/create_user_sso")
async def create_user_sso(request: Request):
    data = await request.json()
    ob_data = c_master_user()
    return await ob_data.create_user_sso(data)


@app.get("/api/f_setting/c_master_user/get_user_salesman")
async def get_user_salesman(param: object = Query(None, alias="param")):
    data_where = json.loads(param)
    ob_data = c_master_user()
    return await ob_data.get_user_salesman(data_where)
