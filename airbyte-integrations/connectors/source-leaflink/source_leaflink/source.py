#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import parse

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# Basic full refresh stream


class LeaflinkStream(HttpStream, ABC):
    primary_key = "id"
    url_base = ""

    def __init__(self, base_url: str, **kwargs):
        super().__init__(**kwargs)
        self.url_base = base_url

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        return [response.json()]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        return next_page_token

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        nextPageToken = response.json().get("next")

        params = {}

        if nextPageToken:
            parsed = parse.urlsplit(nextPageToken)
            page = parsed.scheme + "://" + parsed.netloc + parsed.path

            params.update(dict(parse.parse_qsl(parse.urlsplit(nextPageToken).query)))

            # print(params)
            # if params.get("offset") == '1000':
            #     return None
            return params
        else:
            return None

    def path(self, **kwargs) -> str:
        return self.__class__.__name__.lower()

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        for record in response.json()["results"]:
            yield record


class Customers(LeaflinkStream):
    pass


class ProductCategories(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "product-categories/"


class ProductLines(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "product-lines/"


class Products(LeaflinkStream):
    pass


class OrdersReceived(LeaflinkStream):
    primary_key = "number"

    def path(self, **kwargs) -> str:
        return "orders-received/"


class LineItems(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "line-items/"


class OrderEventLogs(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "order-event-logs/"


class IncrementalLeaflinkStream(LeaflinkStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


# Source
class SourceLeaflink(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        api_key = config["api_key"]
        base_url = config["base_url"]

        headers = {"Authorization": f"App {api_key}", "Content-Type": "application/json"}
        url = f"{base_url}/orders-received/"

        try:
            session = requests.get(url, headers=headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"], auth_method="App")
        return [
            OrdersReceived(authenticator=auth, base_url=config["base_url"]),
            Customers(authenticator=auth, base_url=config["base_url"]),
            ProductCategories(authenticator=auth, base_url=config["base_url"]),
            ProductLines(authenticator=auth, base_url=config["base_url"]),
            Products(authenticator=auth, base_url=config["base_url"]),
            LineItems(authenticator=auth, base_url=config["base_url"]),
            OrderEventLogs(authenticator=auth, base_url=config["base_url"]),
        ]
