from config import jwt_config, path_config, params
import jwt


class __Auth:

    def __init__(self):

        self.secret_key = jwt_config.jwt_profile["key"]
        self.algoritma = jwt_config.jwt_profile["algoritma"]
        self.__user_id = None

    def __check_path(self, path: str):
        data = path_config.path_routes_not_auth
        for pathx in data:
            # print(pathx)
            if path.find(pathx) != -1:
                return False
        return True

    def validate(self, token, path):
        challenges = ['Token type="JWT"']
        # print(path)
        # print(self.__check_path(path))
        # print(not self.__valid(token))

        if not self.__valid(token) and self.__check_path(path):
            return False

        return True

    def create_token(self, payload_data):
        # print('payload_data =',payload_data)
        # print(self.secret_key)
        # print(self.algoritma)

        payload = {
            "username": payload_data["username"],
        }
        # print(payload_data["nik"])
        token = jwt.encode(payload, self.secret_key, self.algoritma)
        return {
            "token": token,
            "user_data": {
                "id_user": payload_data["id_user"],
                "username": payload_data["username"],
                "company_id": payload_data["company_id"],
                "company_name": payload_data["company_name"],
                "cabang_id": payload_data["cabang_id"],
                "cabang_name": payload_data["cabang_name"],
                "id_role": payload_data["id_role"],
                "role_data": payload_data["role_data"],
                "is_view_only": payload_data["is_view_only"],
            },
        }

    def __valid(self, token):
        if token is not None:
            try:
                token_auth = str(token).split(" ")[1]
                payload = jwt.decode(token_auth, self.secret_key, self.algoritma)
                params.par = payload
                return True
            except jwt.DecodeError:
                return False
        else:
            return False

    def get_data_params(self, params_):
        return params.par[params_]


AuthAction = __Auth()
