from enum import Enum
import json
import argparse
from elasticsearch import Elasticsearch
import email
from bs4 import BeautifulSoup
import quopri
import zipfile
from io import BytesIO
import tempfile
from pathlib import Path
from typing import Iterator, Tuple

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
class DlStat(Enum):
    TOCHUNK = 1
    NOCHUNKS = 2
    IGNORE = 3 
def index_exists(es, index: str) -> bool:
    return es.indices.exists(index=index)
def get_esclient():
    return Elasticsearch(get_config("elasticsearch_url"))
        
def make_argparser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("index_name", help="Name of the Elastic index to search for DWG files")
    return parser
def iter_zip_entries(zip_path: str) -> Iterator[Tuple[str, bytes]]:
    """
    Yields (internal_zip_path, file_bytes)
    """
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            with z.open(info) as f:
                yield Path(info.filename), f.read()
                
def process_zip_in_memory(zip_path, handler):
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            with z.open(info) as f:
                handler(info.filename, f.read())
def extract_text_from_mhtml(mhtml_file_path):
    """
    Extracts the main body text from an MHTML file.
    """
    with open(mhtml_file_path, 'rb') as fp: # Open in binary mode
        message = email.message_from_bytes(fp.read())

    html_content = ""
    # Walk through all the parts of the MIME message
    for part in message.walk():
        # Check for the main text/html part
        if part.get_content_type() == "text/html":
            # Get the payload and decode if it's quoted-printable
            payload = part.get_payload(decode=True)
            charset = part.get_content_charset() or 'utf-8'
            try:
                html_content = payload.decode(charset)
                break # Assuming the first text/html part is the main content
            except Exception as e:
                print(f"Error decoding part: {e}")
                continue

    if not html_content:
        return "Could not extract HTML content from MHTML file."

    # Use Beautiful Soup to parse the HTML and extract text
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style tags to clean up the text
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()

    # Get the plain text
    plain_text = soup.get_text()

    # Clean up excess whitespace (optional)
    lines = (line.strip() for line in plain_text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)

    return cleaned_text
def search_by_extension(es_client, index_name, extension):
    query = {
        "query": {
            "bool": {
                "should": [
                    {"term": {"file.extension": extension}},
                    {"term": {"extension": extension}}
                ],
                "minimum_should_match": 1
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
            "bool": {
                "should": [
                    {"term": {"file.extension": extension}},
                    {"term": {"extension": extension}}
                ],
                "minimum_should_match": 1
            }
        }
    }
    count_response = es_client.count(index=index_name, body=query)
    return count_response["count"]
            