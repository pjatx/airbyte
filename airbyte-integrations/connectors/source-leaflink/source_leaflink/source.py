#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from airbyte_cdk.logger import AirbyteLogger

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib import parse
from pathlib import Path

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

# Basic full refresh stream


class LeaflinkStream(HttpStream, ABC):
    primary_key = 'id'
    url_base = ""

    def __init__(self, base_url: str, **kwargs):
        super().__init__(**kwargs)
        self.url_base = base_url

    def parse_response(
        self, response: requests.Response,
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

            params.update(dict(parse.parse_qsl(
                parse.urlsplit(nextPageToken).query)))

            return params
        else:
            return None

    def path(self, **kwargs) -> str:
        return self.__class__.__name__.lower()

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        for record in response.json()["results"]:
            yield record


class LeaflinkIncrementalMixin(HttpStream, ABC):
    cursor_field = "modified"

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)
        self._start_time = 1609480800

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with the stream's most recent state object
        and returning an updated state object.
        """
        latest_benchmark = latest_record[self.cursor_field]
        if current_stream_state.get(self.cursor_field):
            return {self.cursor_field: max(latest_benchmark, current_stream_state[self.cursor_field])}
        return {self.cursor_field: latest_benchmark}

    def request_params(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state)
        print(params)
        start_time = self._start_time
        if stream_state.get(self.cursor_field):
            start_time = stream_state[self.cursor_field]
        params.update({"start_time": start_time,
                      "end_time": pendulum.now().int_timestamp})
        return params


class Customers(LeaflinkStream, LeaflinkIncrementalMixin):
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
    primary_key = 'number'

    def path(self, **kwargs) -> str:
        return "product-categories/"


class LineItems(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "line-items/"


class OrderEventLogs(LeaflinkStream):
    def path(self, **kwargs) -> str:
        return "order-event-logs/"


# Source
class SourceLeaflink(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        api_key = config["api_key"]
        base_url = config["base_url"]

        headers = {"Authorization": f"App {api_key}",
                   "Content-Type": "application/json"}
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
            Customers(authenticator=auth, base_url=config["base_url"]),
            # ProductCategories(authenticator=auth, base_url=config["base_url"]),
            # ProductLines(authenticator=auth, base_url=config["base_url"]),
            # Products(authenticator=auth, base_url=config["base_url"]),
            # LineItems(authenticator=auth, base_url=config["base_url"]),
            # OrdersReceived(authenticator=auth, base_url=config["base_url"]),
            # OrderEventLogs(authenticator=auth, base_url=config["base_url"]),
        ]
