"""
This module contains all functions and classes, specific to the Google Play Store. 
Currently it supports to crawl the following information from the Google Play Store:

- App details
- App reviews
- App permissions
- App data safety information

For each of the above mentioned information, the module provides a function to 
fetch the data from the Google Play Store and a Document class, compatible with
the :func:`kraken.core.tasks.pipelines.data_storage_pipeline` class.

An exemplary Stage to crawl the app details is shown below:

```python
from kraken.core.types import Stage
from kraken.core.tasks.pipelines import data_storage_pipeline

from kraken.google_play_store.tasks.requests.detail_request import request_details
from kraken.google_play_store.tasks.sub_tasks.document_factory import document_factory


details_stage = Stage(
    name="details_stage",
    request=request_details.s(),
    pipelines=[
        data_storage_pipeline.s(
            factory_task=document_factory.s()
        )
        target_discovery_pipeline.s(
            target_field="potential_targets",
    ]
)
```
"""
from kraken.core.types import Stage
