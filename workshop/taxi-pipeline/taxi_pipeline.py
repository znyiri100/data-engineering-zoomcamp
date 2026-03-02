import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_source():
    """NYC taxi trips from the Data Engineering Zoomcamp REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }
    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dev_mode=True,
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_source())
    print(load_info)
