from utils import get_esclient
from pathlib import Path
es=get_esclient()

from elasticsearch import helpers
def duplicateWithExt():
    resp=es.search(index="pdfs",query={"multi_match":{"query":"program"}},size=20)
    for hit in resp["hits"]["hits"]:
        original_doc = hit['_source']

        # Duplicate the document and replace .pdf with .doc
        duplicated_doc = {
            **original_doc,
            
        }
        duplicated_doc["path"]["real"]=original_doc["path"]["real"].replace(".pdf", ".doc")

        # Index the duplicated document back into Elasticsearch
        es.index(index="pdfs", body=duplicated_doc)
def stampEqual():
    docs={}
    resp=es.search(index="pdfs",query={"multi_match":{"query":"program"}},size=350)
    for hit in resp["hits"]["hits"]:
        fileurl=hit["_source"]["file"]["url"]
        if fileurl not in docs: 
            docs[fileurl]=[hit["_id"]]
        else:
            docs[fileurl].append(hit["_id"])
    i=50000        
    for k,v in docs.items():
        i+=1
        for s in v:
           
            doc={
                    "data": {
                    "name": i
                    }
                }
            es.update(index="pdfs",id=s,doc=doc)
def addTitles():
    resp=es.search(index="pdfs",query={"multi_match":{"query":"program"}},size=5)
    i=10005   
    for hit in resp["hits"]["hits"]:
        i+=1 
        doc={"title":f"title{i}",
             "grobid":{"title":f"gtitle{i}"},
             "huridocs":{"title":f"htitle{i}"},
             "data":{"title":f"dtitle{i}"}} 
        es.update(index="pdfs",id=hit["_id"],doc=doc)   
def addfilter():
    resp=es.search(index="pdfs",query={"multi_match":{"query":"numerics"}},size=5)
    i=10005   
    for hit in resp["hits"]["hits"]:
        i+=1 
        doc={
             "data":{"topic":f"numerics"}} 
        es.update(index="pdfs",id=hit["_id"],doc=doc)           
def fixextension():
    resp=es.search(index="pdfs",query={"multi_match":{"query":"program"}},size=350)
    for hit in resp["hits"]["hits"]:
        pat=Path(hit['_source']["path"]["real"])
        #print(pat.suffix,hit['_source']["file"]["extension"])  
        doc={
                    "file": {
                    "extension": pat.suffix.replace(".","")
                    }
                }
        print(doc)
        es.update(index="pdfs",id=hit["_id"],doc=doc)         
if __name__=="__main__":
    addfilter()        