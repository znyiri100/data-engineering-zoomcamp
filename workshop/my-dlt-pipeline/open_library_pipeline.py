"""dlt pipeline to load books from the Open Library API.

Docs:
  Search API  – https://openlibrary.org/dev/docs/api#search
  Books API   – https://openlibrary.org/dev/docs/api/books

Two search modes are supported:
  search   – full-text search via /search.json (offset-paginated)
  bibkeys  – lookup by bibkeys via /api/books (single-page, no pagination)
"""

import argparse
from typing import Literal

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

SearchMode = Literal["search", "bibkeys"]


def open_library_pipeline(mode: SearchMode, value: str) -> None:
    if mode == "search":
        endpoint = {
            "path": "search.json",
            "params": {
                "q": value,
                "limit": 100,
            },
            # The response contains a "docs" list with book records
            "data_selector": "docs",
            "paginator": {
                "type": "offset",
                "limit": 100,
                "total_path": "numFound",
            },
        }
    else:  # bibkeys
        endpoint = {
            "path": "api/books",
            "params": {
                "bibkeys": value,
                "format": "json",
                "jscmd": "data",
            },
            # The response is a dict keyed by bibkey; "$.*" selects all values
            "data_selector": "$.*",
            "paginator": {
                "type": "single_page",
            },
        }

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
            # No authentication required for public read access
        },
        "resources": [
            {
                "name": "books",
                "endpoint": endpoint,
            }
        ],
    }

    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
        dataset_name="open_library",
        dev_mode=True,
        progress="log",
    )

    source = rest_api_source(config)
    load_info = pipeline.run(source)
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load books from Open Library")
    parser.add_argument(
        "--type",
        dest="mode",
        choices=["search", "bibkeys"],
        default="search",
        help="Search mode: 'search' (full-text) or 'bibkeys' (lookup). Default: search",
    )
    parser.add_argument(
        "value",
        help=(
            "For --type search: query string, e.g. 'harry potter'. "
            "For --type bibkeys: comma-separated bibkeys, e.g. 'ISBN:0451526538,LCCN:62019420'."
        ),
    )

    args = parser.parse_args()
    open_library_pipeline(args.mode, args.value)
