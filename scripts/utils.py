import json
import argparse
from elasticsearch import Elasticsearch

def get_config(key: str):
    with open("config.json") as f:
        d = json.load(f)
        if key in d:
            return d[key]
        else: 
            raise KeyError(f"Unknown config key {key}")
def fullpath_exists(es, index: str, fullpath: str) -> bool:
    query = {
        "query": {
            "term": {
                "fullpath": fullpath
            }
        },
        "size": 0
    }

    res = es.search(index=index, body=query)
    return res["hits"]["total"]["value"] > 0
def index_exists(es, index: str) -> bool:
    return es.indices.exists(index=index)
def get_esclient():
    return Elasticsearch(get_config("elasticsearch_url"))
        
def make_argparser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("index_name", help="Name of the Elastic index to search for DWG files")
    return parser

def search_by_extension(es_client, index_name, extension):
    query = {
        "query": {
            "term": {
                "file.extension": extension
            }
        }
    }
    response = es_client.search(index=index_name, body=query, scroll="90m")
    scroll_id = response["_scroll_id"]

    while True:
        hits = response["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            yield hit
        response = es_client.scroll(scroll_id=scroll_id, scroll="90m")

def count_files_with_extension(es_client, index_name, extension):
    query = {
    "query": {
        "term": {
        "file.extension": extension
        }
    }
    }
    count_response = es_client.count(index=index_name, body=query)
    return count_response["count"]
            