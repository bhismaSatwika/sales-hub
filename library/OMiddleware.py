from fastapi import Request, HTTPException, status, responses, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from library.router import app
import time
from library.auth import AuthAction


# def check_path(self, path: str):
#     data = path_config.path_routes_not_auth
#     for pathx in data:
#         # print(pathx)
#         if path.find(pathx) != -1:
#             return False
#     return True

# app = router.app

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    token = request.headers.get("Authorization")
    app_version = request.headers.get("X-App-Version")
    base_path = request.base_url
    path = str(request.url).replace(str(base_path), "")
    # print(path)

    status_, code = await AuthAction.validate(token, path, app_version)

    if status_:
        return await call_next(request)
    else:
        if code == "426":
            return responses.JSONResponse(
                status_code=status.HTTP_426_UPGRADE_REQUIRED,
                content="Versi tidak sesuai, mohon refresh halaman",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return responses.JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
