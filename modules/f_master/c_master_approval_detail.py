from datetime import datetime
import json
from fastapi import HTTPException, Query, Request
from library import *
import os
from library.router import app
from library.db import Db


class c_master_approval_detail(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self,
        orderby,
        limit,
        offset,
        filter,
        id_header,
        filter_other="",
        filter_other_conj="",
    ):
        if orderby == None or orderby == "":
            orderby = "updateindb desc"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        query = f"""
        SELECT * FROM (
            SELECT A.id_ as id_header, B.id, E.company_name, F.cabang_name, C.name as issued_by, D.name as approver, B.updateindb, B.approval_order FROM master_approval_header A
            LEFT JOIN master_approval B on A.id_ = B.header_id
            LEFT JOIN master_user C on B.issued_by = C.username
            LEFT JOIN master_user D on B.username = D.username
            LEFT JOIN master_company E on E.id_company = C.company_id 
            LEFT JOIN master_company_cabang F on C.company_id = F.id_company AND C.cabang_id = F.id_cabang 
            WHERE A.id_ = '{id_header}'
        ) X
        """

        sql = query + str_clause
        sql2 = query + str_clause_count

        sql_count = f"""SELECT COUNT(*) FROM ({sql2}) as count"""

        result = await self.db.executeToDict(sql)
        result_count = await self.db.executeToDict(sql_count)

        data = {"data": result, "count": result_count[0]["count"]}
        return data

    async def create(self, data):

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlString = self.db.genStrInsertSingleObject(data, "master_approval_detail")

        try:
            # print(sqlString)
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            # print(e)
            message = {"status": "error : " + str(e)}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def update(self, data, data_where):

        data.update(
            {
                "userupdate": auth.AuthAction.get_data_params("username"),
                "updateindb": datetime.today(),
            }
        )

        sqlString = self.db.genUpdateObject(data, data_where, "master_approval_detail")
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_approval_detail")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_approval_master(self, id_header):
        sql = f"""
            SELECT active, release FROM master_approval_header
        WHERE id_ = '{id_header}'
        """

        result = await self.db.executeToDict(sql)
        result = result[0]
        return result


@app.get("/api/f_master/c_master_approval_detail/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
    id: str = Query(None, alias="id"),
):
    ob_data = c_master_approval_detail()
    return await ob_data.read(orderby, limit, offset, filter, id)


@app.get("/api/f_master/c_master_approval_detail/get_master_detail")
async def get_detail(id):
    ob_data = c_master_approval_detail()
    return await ob_data.get_approval_master(id)


@app.post("/api/f_master/c_master_approval_detail/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_approval_detail/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_approval_detail/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_approval_detail()
    return await ob_data.delete(data)


@app.get("/api/f_master/c_master_approval_detail/get_approval_detail")
async def get_approval_detail():
    ob_data = c_master_approval_detail()
    return await ob_data.get_approval_detail()


@app.get("/api/f_master/c_master_approval_detail/get_approval_detail_where_condition")
async def get_approval_header_where_condition(
    param: object = Query(None, alias="param")
):
    data_where = json.loads(param)
    ob_data = c_master_approval_detail()
    return await ob_data.get_approval_detail_where_condition(data_where)


@app.get("/api/f_master/c_master_approval_detail/get_atribut_approval_detail")
async def get_atribut_approval_detail(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_approval_detail()
    return await ob_data.get_atribut_approval_detail(data)
