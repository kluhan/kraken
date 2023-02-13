import datetime

from kraken.core.types import Series, Crawl
from kraken.utils.mongodb import MongoEngineContextManager

import pytest


TIMESTAMP = datetime.datetime.now()


def test_get_filter():
    # Test 1: Check if single "or" key is restored to "$or"
    series = Series(name="test", filter={"or": [1, 2, 3]})
    assert series.get_filter() == {"$or": [1, 2, 3]}

    # Test 2: Check if single "and" key is restored to "$and"
    series = Series(name="test", filter={"and": [1, 2, 3]})
    assert series.get_filter() == {"$and": [1, 2, 3]}

    # Test 3: Nested filter with multiple levels
    series = Series(name="test", filter={"or": {"and": {"or": [1, 2, 3]}}})
    modified_dict = series.get_filter()
    assert modified_dict == {"$or": {"$and": {"$or": [1, 2, 3]}}}

    # Test : Nested filter with single level
    series = Series(name="test", filter={"or": {"key1": {"key2": [1, 2, 3]}}})
    modified_dict = series.get_filter()
    assert modified_dict == {"$or": {"key1": {"key2": [1, 2, 3]}}}

    # Test 3: filter with no 'or' or 'and' keys
    series = Series(name="test", filter={"key1": {"key2": {"key3": [1, 2, 3]}}})
    modified_dict = series.get_filter()
    assert modified_dict == {"key1": {"key2": {"key3": [1, 2, 3]}}}

    # Test 4: filter with 'or' key at the top level only
    series = Series(name="test", filter={"or": {"key1": {"key2": [1, 2, 3]}}})
    modified_dict = series.get_filter()
    assert modified_dict == {"$or": {"key1": {"key2": [1, 2, 3]}}}

    # Test 5: Empty filter
    series = Series(name="test", filter={})
    modified_dict = series.get_filter()
    assert modified_dict == {}


def test_set_filter():
    # Test 1: Check if single "$or" key is set to "or"
    series = Series(name="test", filter={})
    series.set_filter({"$or": [1, 2, 3]})
    assert series.filter == {"or": [1, 2, 3]}

    # Test 2: Check if single "$and" key is set to "and"
    series = Series(name="test", filter={})
    series.set_filter({"$and": [1, 2, 3]})
    assert series.filter == {"and": [1, 2, 3]}

    # Test 3: Nested filter with multiple levels
    series = Series(name="test", filter={})
    series.set_filter({"$or": {"$and": {"$or": [1, 2, 3]}}})
    assert series.filter == {"or": {"and": {"or": [1, 2, 3]}}}

    # Test : Nested filter with single level
    series = Series(name="test", filter={})
    series.set_filter({"$or": {"key1": {"key2": [1, 2, 3]}}})
    assert series.filter == {"or": {"key1": {"key2": [1, 2, 3]}}}

    # Test 3: filter with no 'or' or 'and' keys
    series = Series(name="test", filter={})
    series.set_filter(filter={"key1": {"key2": {"key3": [1, 2, 3]}}})
    assert series.filter == {"key1": {"key2": {"key3": [1, 2, 3]}}}

    # Test 4: filter with '$or' key at the top level only
    series = Series(name="test", filter={})
    series.set_filter(filter={"$or": {"key1": {"key2": [1, 2, 3]}}})
    assert series.filter == {"or": {"key1": {"key2": [1, 2, 3]}}}

    # Test 5: Empty filter
    series = Series(name="test", filter={})
    series.set_filter(filter={})
    assert series.filter == {}
