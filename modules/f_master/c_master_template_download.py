from datetime import datetime
import json
import mimetypes
from fastapi import HTTPException, Query, Request
from fastapi.responses import FileResponse
from config import params
from library import *
import os
from library.router import app
from library.db import Db


class c_master_template_download(object):
    def __init__(self):
        self.db = Db()
        self.kendoParse = kendo_parse.KendoParse

    async def read(
        self, orderby, limit, offset, filter, filter_other="", filter_other_conj=""
    ):
        if orderby == None or orderby == "":
            orderby = "updateindb desc"
        str_clause = self.kendoParse().parse_query(
            orderby, limit, offset, filter, filter_other, filter_other_conj
        )
        str_clause_count = self.kendoParse().parse_query(
            "", None, None, filter, filter_other, filter_other_conj
        )

        sql = (
            f"""SELECT *
                FROM master_template_download"""
            + str_clause
        )

        sql_count = (
            f"""SELECT count(*) as count 
                FROM master_template_download"""
            + str_clause_count
        )

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

        sqlString = self.db.genStrInsertSingleObject(data, "master_template_download")

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

        sqlString = self.db.genUpdateObject(
            data, data_where, "master_template_download"
        )
        # print(sqlString)
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def delete(self, data_where):
        sqlString = self.db.genDeleteObject(data_where, "master_template_download")
        try:
            await self.db.executeQuery(sqlString)
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))
        return message

    async def get_template_download_list(self):
        sql = f"""SELECT id as value,file_name as text 
				    FROM master_template_download 
                    WHERE status_release = 't' AND status_aktif = 't'
                    ORDER BY id ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result

    async def get_template_download_where_condition(self, where_condition):
        if where_condition != None:
            where_sql = f"""WHERE {where_condition['where_condition']} AND status_release = 't' AND status_aktif = 't'"""
        else:
            where_sql = f"""WHERE (1=1)"""

        sql = f"""SELECT id as value,file_name as text 
    				FROM master_template_download {where_sql} ORDER BY id ASC"""
        result = await self.db.executeToDict(sql)
        # print(result)
        return result

    async def get_atribut_template_download(self, id_biaya):
        sql = f"""SELECT id as value,file_name as text,* FROM master_template_download 
                WHERE id = {id_biaya} AND status_release = 't' AND status_aktif = 't' LIMIT 1"""
        result = await self.db.executeToDict(sql)
        data = {"data": result}

        # print(sql)
        return data

    async def get_template_download(self):
        sql = f"""SELECT file_name FROM master_template_download WHERE status_release = 't' AND status_aktif = 't'"""
        try:
            result = await self.db.executeToDict(sql)
            # print(result)
            if len(result) == 0:
                return 0
            message = {"status": "success"}
        except Exception as e:
            message = {"status": "error"}
            raise HTTPException(400, ("The error is: ", str(e)))

        return result[0]["file_name"]

    async def read_files(self, id):
        sql = f""" SELECT file_name, nama FROM master_template_download where id = '{id}' """

        result = await self.db.executeToDict(sql)

        output_list = [
            {"file_name": item["file_name"], "file": {"name": item["files"]}}
            for item in result
        ]
        return output_list

    def get_content_type(self, file_path):
        # Get the MIME type based on the file extension
        mime_type, _ = mimetypes.guess_type(file_path)
        # If the MIME type cannot be guessed, fallback to 'application/octet-stream'
        return mime_type if mime_type else "application/octet-stream"

    async def stream_file(self, path, filename):
        try:
            content_type = self.get_content_type(path)
            return FileResponse(path, media_type=content_type, filename=filename)
        except Exception as e:
            raise HTTPException(400, "The error is: " + str(e))


"""
list your path url at bottom
example /testing url
test from postman :
url/api/c_master_template_dwonload/testing
for post method and other method, check tutorial from 
https://fastapi.tiangolo.com/
"""


@app.get("/api/f_master/c_master_template_download/read")
async def read_data(
    limit: int = Query(None, alias="$top"),
    orderby: str = Query(None, alias="$orderby"),
    offset: int = Query(None, alias="$skip"),
    filter: str = Query(None, alias="$filter"),
):
    # print("the data:", nik, limit, orderby, offset, filter)
    ob_data = c_master_template_download()
    return await ob_data.read(orderby, limit, offset, filter)


@app.post("/api/f_master/c_master_biaya_admin/create")
async def create_data(request: Request):
    data = await request.json()
    ob_data = c_master_template_download()
    return await ob_data.create(data)


@app.post("/api/f_master/c_master_template_download/update")
async def update_data(request: Request):
    data = await request.json()
    ob_data = c_master_template_download()
    return await ob_data.update(data["update_data"], data["update_where"])


@app.post("/api/f_master/c_master_template_download/delete")
async def delete(request: Request):
    data = await request.json()
    ob_data = c_master_template_download()
    return await ob_data.delete(data)


@app.get(
    "/api/f_master/c_master_template_download/get_template_download_where_condition"
)
async def get_template_download_where_condition(
    param: object = Query(None, alias="param")
):
    data_where = json.loads(param)
    ob_data = c_master_template_download()
    return await ob_data.get_template_download_where_condition(data_where)


@app.get("/api/f_master/c_master_template_download/get_atribut_template_download")
async def get_atribut_template_download(param: object = Query(None, alias="param")):
    # print('MASUKKKKKK')
    data = json.loads(param)
    ob_data = c_master_template_download()
    return await ob_data.get_atribut_template_download(data)


@app.get("/api/f_master/c_master_template_download/get_template_download")
async def get_template_download():
    ob_data = c_master_template_download()
    return await ob_data.get_template_download()


@app.get("/api/f_master/c_master_template_download/read_files")
async def get_td_files(id_trans: str = Query(None, alias="id_trans")):
    ob_data = c_master_template_download()
    return await ob_data.read_files(id_trans)


@app.get("/api/f_master/c_master_template_download/stream_file")
async def stream_file(filename: str = Query(None, alias="filename")):
    ob_data = c_master_template_download()
    path_parent = params.loc["file_template_download"]
    path = path_parent + "/" + filename
    return await ob_data.stream_file(path, filename)
