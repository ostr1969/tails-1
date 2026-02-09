import argparse
from time import time
from docling.datamodel.base_models import InputFormat
import json,os
from pypdf import PdfReader
import utils

from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableStructureOptions,TesseractOcrOptions
)
from langchain_docling import DoclingLoader

from docling.chunking import HybridChunker
from langchain_docling.loader import ExportType
from tempfile import mkdtemp

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
from pathlib import Path



pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = True
pipeline_options.table_structure_options = TableStructureOptions(
    do_cell_matching=True
)

pipeline_options.ocr_options.lang = ["heb"]
pipeline_options.accelerator_options = AcceleratorOptions(
    num_threads=4, device=AcceleratorDevice.AUTO
)

converter = DocumentConverter(
    format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
)
doc_store = {}
doc_store_root = Path(mkdtemp())
chunker = HybridChunker(tokenizer=EMBED_MODEL_ID,max_tokens=350)
def pdf_loader(file_path: str):
    doc={}
    reader = PdfReader(file_path)
    meta = reader.metadata
    doc["metadata"] = meta
    if meta.get("/Title"):
        doc["title"] = meta.get("/Title")
    dl_doc = converter.convert(source=file_path).document
    doc["fulltext"]=dl_doc.export_to_markdown()
    #file_path = Path(doc_store_root / f"{dl_doc.origin.binary_hash}.json")
    #dl_doc.save_as_json(file_path)
    #doc_store[dl_doc.origin.binary_hash] = file_path
    chunks=chunker.chunk(dl_doc)
    doc["chunks"]=[]
    for i, chunk in enumerate(chunks):
        chunk_dict={}
        filename=chunk.meta.origin.filename
        text=chunk.text
        #chunk_dict["filename"]=filename
        chunk_dict["text"]=text
        chunk_dict["items"]=[]
        for item in chunk.meta.doc_items:
            item_dict={}
            item_dict["content_layer"]=item.content_layer.name
            item_dict["label"]=item.label.name
            item_dict["prov"]=[]
            for prov in item.prov:
                prov_dict={}
                prov_dict["page_no"]=prov.page_no
                bb=prov.bbox
                prov_dict["bbox"]={"l":bb.l,"t":bb.t,"r":bb.r,"b":bb.b}
                item_dict["prov"].append(prov_dict)
            chunk_dict["items"].append(item_dict)
        doc["chunks"].append(chunk_dict)
    return doc       



# loader = DoclingLoader(
#     file_path=SOURCES,
#     converter=converter,
#     export_type=ExportType.DOC_CHUNKS,
#     chunker=HybridChunker(tokenizer=EMBED_MODEL_ID,max_tokens=350),
    
# )

#docs = loader.load()  
# n=30
# for n in range(16, 18):
#     print(docs[n].metadata["dl_meta"]["origin"]["filename"], " chunk ", n," content length:", len(docs[n].page_content))
#     print(      docs[n].page_content)
#     for item in docs[n].metadata["dl_meta"]["doc_items"]:
#         print("content layer:",item["content_layer"]," label:",item["label"])
#         for prov in item["prov"]:
#             bbox=prov.get("bbox",None)
#             print("page:",prov["page_no"],",bbox:",int(bbox["l"]), int(bbox["t"]), int(bbox["r"]), int(bbox["b"]))
#     print(docs[n].metadata)
   
if __name__ == "__main__":
    description="Index all files in a directory to an index using FSCrawler"
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument("index_name", help="Name of the Elastic index to create")
    parser.add_argument("path", help="path to the indexed directory")
    parser.add_argument("--new", help="Delete index name and create new one", action="store_true")
    args = parser.parse_args()
    EsClient=utils.get_esclient()
    exts = {".pdf", ".docx"}
    folder=Path(args.path)
    files = list(folder.rglob("*"))
    files = [p for p in files if p.suffix.lower() in exts]
    with open("doclingMap.json") as f:
        mappings = json.load(f)
    if args.new:
        print(f"Deleting index {args.index_name} if exists")
        EsClient.indices.delete(index=args.index_name, ignore=[400, 404])
        EsClient.indices.create(index=args.index_name,body=mappings)
    if not utils.index_exists(EsClient,args.index_name):
        print(f"Index {args.index_name} does not exist, creating")
        EsClient.indices.create(index=args.index_name,body=mappings)    
    for f in files:
        doc = str(f.resolve())
        if utils.fullpath_exists(EsClient,args.index_name,doc):
            print(f"skipping {doc}, already indexed")
            continue
        start_time=time()
        result=pdf_loader(doc)
        result["fullpath"]=doc
        result["extension"]=f.suffix.lower().lstrip(".")

        EsClient.index(index=args.index_name, body=result)
        
        print(f"indexed {doc}, took {time()-start_time:.2f} seconds")
        