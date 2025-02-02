#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from .core import HttpAuthenticator


class LeaflinkAuthenticator(HttpAuthenticator):
    def __init__(self, token: str, auth_method: str = "Bearer", auth_header: str = "Authorization"):
        self.auth_method = auth_method
        self.auth_header = auth_header
        self._token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {self.auth_header: f"{self.auth_method} {self._token}"}
