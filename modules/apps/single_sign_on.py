import base64

from fastapi import HTTPException, Query
import httpx
from library import *
import os
from library.router import app
from library.db import Db
from pydantic import BaseModel
from modules.apps.login import login as Login


class single_sign_on(object):
    def __init__(self):
        self.db = Db()
        self.apps_id = "1d3b256b98957acf5b285622a1301c62"
        self.url = "http://170.1.20.110:8051"

    async def req_wa_auth(self, nik):
        api_first_token_access = f"{self.url}/api/sso/req/get_first_token_access"
        api_req_wa_auth = f"{self.url}/api/sso/req/req_wa_auth"

        try:
            sql_get_user = f"SELECT * FROM public.master_user WHERE username='{nik}'"
            result = await self.db.executeToDict(sql_get_user)
            if result == []:
                raise HTTPException(400, {"message": "User not found"})
        except Exception as e:
            raise HTTPException(400, str(e))

        try:
            async with httpx.AsyncClient() as client:
                # Get the first token access
                first_token_response = await client.get(
                    api_first_token_access,
                    params={"apps_id": self.apps_id},
                )
                if first_token_response.status_code != 200:
                    raise HTTPException(
                        500,
                        {
                            "status": "error",
                            "message": "Failed to get first token access",
                        },
                    )

                try:
                    first_token_data = first_token_response.json()
                except ValueError:
                    raise HTTPException(
                        500,
                        {
                            "status": "error",
                            "message": "Invalid JSON response from first token API",
                        },
                    )

                # Get the WA authentication
                wa_auth_response = await client.get(
                    api_req_wa_auth,
                    params={
                        "apps_id": self.apps_id,
                        "token_access": first_token_data.get("token_access"),
                        "userid_or_email": nik,
                    },
                )
                if wa_auth_response.status_code != 200:
                    return {
                        "status": "error",
                        "message": "Failed to get WA auth",
                    }

                try:
                    wa_auth_data = wa_auth_response.json()
                except ValueError:
                    raise HTTPException(
                        500,
                        {
                            "status": "error",
                            "message": "Invalid JSON response from WA auth API",
                        },
                    )

                if wa_auth_data.get("status") == "success":
                    return {
                        "status": "success",
                        "token_access_user": wa_auth_data.get("token_access_user"),
                    }
                else:
                    raise HTTPException(
                        500,
                        {
                            "status": "error",
                            "message": "Failed to get WA auth",
                        },
                    )

        except httpx.RequestError as exc:
            raise HTTPException(
                500,
                {
                    "status": "error",
                    "message": f"An error occurred while requesting: {exc}",
                },
            )

    async def verify_user(self, nik, token_access_user, token_code):
        api_req_wa_auth = f"{self.url}/api/sso/req/verify"

        try:
            async with httpx.AsyncClient() as client:
                # verifying token code
                verify_response = await client.get(
                    api_req_wa_auth,
                    params={
                        "token_access": token_access_user,
                        "token_code": token_code,
                        "userid_or_email": nik,
                        "apps_id": self.apps_id,
                    },
                )

                print("verify_response: ", verify_response)

                if verify_response.status_code != 200:
                    raise HTTPException(500, "Error")

                try:
                    verify_data = verify_response.json()
                except ValueError:
                    raise HTTPException(
                        500,
                        {
                            "status": "error",
                            "message": "Invalid JSON response from first token API",
                        },
                    )
                print("verify_data: ", verify_data)

                if verify_data.get("status") == "success":
                    login = Login()
                    data = await login.login(nik)
                    return data
                else:
                    raise HTTPException(
                        400,
                        {
                            "status": "error",
                            "message": verify_data.get("desc"),
                        },
                    )

        except httpx.RequestError as exc:
            return {
                "status": "error",
                "message": f"An error occurred while requesting: {exc}",
            }


@app.get("/api/apps/single_single_on/req_wa_auth")
async def get_data(
    nik: str = Query(None, alias="nik"),
):
    ob_data = single_sign_on()
    return await ob_data.req_wa_auth(nik)


@app.get("/api/apps/single_single_on/verify_user")
async def get_data(
    nik: str = Query(None, alias="nik"),
    token_access_user: str = Query(None, alias="token_access_user"),
    token_code: int = Query(None, alias="token_code"),
):
    ob_data = single_sign_on()
    return await ob_data.verify_user(nik, token_access_user, token_code)
