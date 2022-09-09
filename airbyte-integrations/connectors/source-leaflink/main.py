#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_leaflink import SourceLeaflink

if __name__ == "__main__":
    source = SourceLeaflink()
    launch(source, sys.argv[1:])
