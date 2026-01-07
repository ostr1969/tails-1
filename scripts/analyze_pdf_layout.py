import argparse
import xml.etree.ElementTree as ET
import json
from grobid2json import convert_xml_to_json
from bs4 import BeautifulSoup
from typing import List
import requests
import os
import bibtexparser
from concurrent.futures import ThreadPoolExecutor, TimeoutError

def raw_layout_json(path: str) -> dict:
    """
    Sends a PDF file to a local API endpoint and returns the JSON response.
    
    Args:
        path: Path to the PDF file
        
    Returns:
        JSON response as a dictionary
    """
    url = "http://localhost:5060"
    
    # Verify file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF file not found: {path}")
    
    # Prepare the multipart form data
    with open(path, 'rb') as pdf_file:
        files = {'file': pdf_file}
        data = {'fast': 'true'}
        
        response = requests.post(url, files=files, data=data)
        response.raise_for_status()
        
        return response.json()
    
def get_title(layout: List[dict], nsegments: int=2):
    sections = {
        "title": [],
        "section header": [],
    }
    for seg in layout:
        t = seg.get("type").lower()
        if t in sections:
            sections[t].append(seg.get("text"))
    if sections["title"]:
        return " ".join(sections["title"])
    else:
        return max(sections["section header"][:nsegments], key=len) if len(sections["section header"]) > 0 else None

def clean_content(layout: List[dict]):
    content = ""
    for seg in layout:
        t = seg.get("type").lower()
        if t in ["title", "section header", "text", "footnote", "caption"]:
            content += seg.get("text") + "\n"
    return content


def grobid_bibliographic_data(path: str):
    """
    Sends a PDF file to a local API endpoint and returns the JSON response.
    
    Args:
        path: Path to the PDF file
        
    Returns:
        JSON response as a dictionary
    """
    url = "http://localhost:8070/api/processHeaderDocument"
    #url = "http://localhost:8070/api/processFulltextDocument"
    
    # Verify file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF file not found: {path}")
    
    # Prepare the multipart form data
    with open(path, 'rb') as pdf_file:
        files = {'input': pdf_file}        
        headers = {'Accept': 'application/xml'}
        files = {
        "input": (os.path.basename(path), pdf_file, "application/pdf")
        }
        response = requests.post(url, files=files)
        response.raise_for_status()  
        #print(response.text) 
        #print(response.headers.get("Content-Type"))    
        #soup = BeautifulSoup(response.text, "xml")  
        #print(soup)
        bib_database = bibtexparser.loads(response.text)      
        #paper = convert_xml_to_json(soup, paper_id="id", pdf_hash="hash")
        #print(json.dumps(bib_database.entries[0], indent=2))
        entry = bib_database.entries[0]
        if "abstract" in entry:
            entry["abstract"] = {"text": entry["abstract"]}
        if "date" in entry and (entry["date"] is None or entry["date"].strip() == ""):
            del entry["date"]    
        return entry

def run_with_timeout(func, args, timeout=30):
    """
    Runs a function with arguments in a try-catch block with a timeout.
    
    Args:
        func: Function to execute
        args: Arguments to pass to the function (list or tuple)
        timeout: Timeout in seconds (default: 30)
        
    Returns:
        Function result or None if it fails
    """
    
    try:
        with ThreadPoolExecutor() as executor:
            future = executor.submit(func, *args)
            return future.result(timeout=timeout)
    except TimeoutError:
        print(f"Function {func.__name__} timed out after {timeout} seconds")
        return None
    except Exception as e:
        print(f"Error executing {func.__name__}: {e}")
        return None

def analyze_file(path: str, file_timeout: float):
    
    
    pdla_layout = run_with_timeout(raw_layout_json, (path, ), file_timeout)
    glayout=run_with_timeout(grobid_bibliographic_data, (path, ), file_timeout)
    
    return {
        "huridocs": {
            "layout": pdla_layout,
            "clean_content": clean_content(pdla_layout) if pdla_layout else None,
            "title": get_title(pdla_layout) if pdla_layout else None
        },
        "grobid": glayout
    }


if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import utils
    parser = argparse.ArgumentParser(description="analyze pdf layout in the index")
    parser.add_argument("index_name", type=str, help="name of the Elastic index to analyze")
    parser.add_argument("file_timeout", type=float, help="timeout for analyzing file")
    parser.add_argument("--restart", action="store_true", help="whether to restart the analysis from scratch")
    args = parser.parse_args()
    es = utils.get_esclient()
    index_name = args.index_name 

    nfiles = utils.count_files_with_extension(es, index_name, "pdf")
    print(f"Start analyzing layout of {nfiles}...")
    for i, hit in enumerate(utils.search_by_extension(es, index_name, "pdf")):
        doc_id = hit["_id"]
        path = hit["_source"]["path"]["real"]
        print(f"analyzing {doc_id} {i + 1} out of {nfiles} ({path})")        

        if (("grobid" in hit["_source"] or "huridocs" in hit["_source"]) and not args.restart):
            print("  already analyzed, skipping")
            continue
        

        analysis = analyze_file(path, args.file_timeout)
        #grobid_bibliographic_data(path)
        # Update document in Elasticsearch
        es.update(index=index_name, id=doc_id, body={"doc": analysis})

    print(f"Successfully analyzed {nfiles} documents")
