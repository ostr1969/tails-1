from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from logging import root
from os import walk
from typing import List
from __init__ import CONFIG
from collections.abc import Iterable
from bs4 import BeautifulSoup
from utils import build_pagespan_map

def clean_html_keep_em_highlight(text: str) -> str:
        soup = BeautifulSoup(text, "html.parser")

        for tag in soup.find_all(True):
            if tag.name == "em" and tag.get("class") == ["highlight"]:
                continue
            tag.unwrap()

        return str(soup)

def merge_page_bounds(page_bounds_list):
    acc = defaultdict(list)

    # collect all page boxes
    for page_bounds in page_bounds_list:
        for page, bbox in page_bounds.items():
            acc[page].append(bbox)

    # merge per page
    merged = {}
    for page, boxes in acc.items():
        merged[page] = {
            "l": min(b["l"] for b in boxes),
            "t": max(b["t"] for b in boxes),
            "r": max(b["r"] for b in boxes),
            "b": min(b["b"] for b in boxes),
        }

    return merged
def bounding_boxes_by_page(prov):
    pages = defaultdict(list)
    #print("pp:",prov)
    # group boxes by page
    for item in prov:

        #print(item["page_no"])
        pages[item["page_no"]].append(item["bbox"])

    # compute bounding square per page
    result = {}

    for page, boxes in pages.items():
        #print("boxes",boxes)
        result[page] = {
            "l": min(b["l"] for b in boxes),
            "t": max(b["t"] for b in boxes),
            "r": max(b["r"] for b in boxes),
            "b": min(b["b"] for b in boxes),
        }

    return result


@dataclass
class SearchHit:

    hit: dict
    display_fields: dict
    chunk_dict: dict
    chunkids: list
    title_fields: list
    extension: str

 

    def get_field_value_obselete(self, field: str):
        """get the field value. field given in plain text"""
        res = deepcopy(self.hit["_source"])
        ajr = field.split(".")
        for s in ajr:
            if s in res:
                res = res[s]
            else:
                return None
            if not res:
                return res
        return res
    def get_field_value(self, field: str, join_with=" "):
        parts = field.split(".")
        root = deepcopy(self.hit["_source"])

        def walk(value, idx):
            if idx == len(parts):
                return [value]

            key = parts[idx]
            results = []

            if isinstance(value, dict):
                if key in value:
                    results.extend(walk(value[key], idx + 1))

            elif isinstance(value, list):
                for item in value:
                    results.extend(walk(item, idx))

            return results

        values = walk(root, 0)

    # remove None / empty
        values = [v for v in values if v not in (None, "", [])]

        if not values:
            return None

        if len(values) == 1:
            return values[0]

        return join_with.join(map(str, values))
    
    def has_field(self, field: str):
        return self.get_field_value(field) is not None
    
    def get_file_url(self) -> str:
        """Method to get the file URL for a hit. this is to be used in links"""
        url = self.get_field_value("file.url")
        if not url is None:
            url = url.replace("file://", "file:///")
        return url

    def hit_to_table(self):
        table_rows = []
        for display in self.display_fields:
            # read field value from dictionary
            field_value = str(self.get_field_value(display["field"]))
            if field_value is None or field_value=="None" or field_value=="" or field_value=="[]":
                continue
            # in case we need to use highlighted format, read the value from highlights
            if "use_highlights" in display  and "highlight" in self.hit and display["field"] in self.hit["highlight"]:
                field_value = "...".join(self.hit["highlight"][display["field"]])
            if "max_length" in display and len(field_value) > display["max_length"]:
                field_value = field_value[:display["max_length"]] + "..."
            if  display["field"]=="content":
                field_value=  clean_html_keep_em_highlight(field_value) 
            # format field according to styling information
            formatted = display["style"].replace("$VALUE", field_value)
            # collect data to table
            if display["field"] in CONFIG["filter_fields"]:
                filter=display["field"]
            else:
                filter=""
            table_rows.append([display["display_name"], formatted,filter])
        titles=[]   
        for display in self.title_fields:
                field_value = str(self.get_field_value(display))
                if field_value is None or field_value=="None" or field_value=="" or field_value=="[]":
                    continue
                titles.append(field_value)
        return titles,table_rows
    
    def hit_title(self):
        extention = str(self.get_field_value("file.extension")).upper()
        return "<a href=/view/{}/{} class=\"document-title\">{} file</a>".format(self.hit["_index"], self.hit["_id"], extention)
    def hit_chunks(self):
        chunks=[]

        for chunk in self.hit.get("s_chunks", []):#selected matched chanks
            print(chunk)
            pages=[]
            provboxes=[]
            for item in chunk.get("items",[]):

                prov=  item.get("prov", [])


                pbound=bounding_boxes_by_page(prov)
                provboxes.append(pbound)
            itemsbound=merge_page_bounds(provboxes)

            chunk["bounds"]=itemsbound

            #print("text:",chunk["text"],"bound:",itemsbound)
            chunks.append(itemsbound)
        print("chunks:",chunks)
        return chunks


    def make_html(self) -> str:
        """Make required html for presenting the hit in the search resutls"""
        titles,table_rows = self.hit_to_table()
        # convert table to HTML
        s = "<table class=\"document-table\">"
        if len(titles)>0:
            #print(titles)
            s += f'''<tr><td class=\"key\">title</td><td class=\"tvalue\">{titles[0]}
                <div class="tooltip">
                        <div class="tooltip-text">{ "\n".join(titles) }</div>
                </div>
            </td></tr>'''
        for row in table_rows:
            
            if row[2]:
                 route=f"/filter/{self.hit["_id"]}/{row[2]}"
                 s += '<tr><td class="key">{}</td><td class="value"><a class="link" href="{}">{}</a></td></tr>'.format(
                     row[0],route,row[1]
                            )
            else:
                s += "<tr><td class=\"key\">{}</td><td class=\"value\">{}</td></tr>".format(row[0], row[1])
        s += "</table>"
        #print(s+"\n")
        
        for i,c in enumerate(self.chunk_dict.items()):
            pages=c[1][0]
            text=c[1][1]
            s+= f'''<div class="counter">
            <a href="/view/{self.hit["_index"]}/{self.hit["_id"]}#page={pages[0]}" class="counter-link">
                {i}
            </a>
            <div class="tooltip">
                    <div class="tooltip-pages">Pages: {pages }</div>
                    <div class="tooltip-text">{ text }</div>
            </div>
             </div>
            '''

        return s

def hits_from_resutls(results) -> List[SearchHit]:
    ajr = []
    #for hit in results:
    #    ajr.append(SearchHit(hit, CONFIG["display_fields"],{},[]))
    for hit in results:
        if "s_chunks" in hit:
            chunkids=[u["id"] for u in hit["s_chunks"]]
            #print(chunkids)
            chunksdict=build_pagespan_map(hit["s_chunks"]) #{(3,4,):asdfaf,{5,6}:sdfasftrrty}
        else:
            chunkids=[]
            chunksdict={}
        ext=hit["_source"]["file"]["extension"]
        ajr.append(SearchHit(hit, CONFIG["display_fields"],chunksdict,chunkids,CONFIG["title_fields"],ext))

    return ajr
