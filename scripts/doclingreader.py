import argparse,pypandoc
import json
import tempfile #TODO add pypandoc and pypandoc_binary to image
from pathlib import Path
import docx
from openpyxl import load_workbook
from pptx import Presentation
from langchain_docling.loader import DoclingLoader
from docling.chunking import HybridChunker
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import ConversionResult
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
    WordFormatOption,
    CsvFormatOption,
    AsciiDocFormatOption
)
from docling.backend.msword_backend import MsWordDocumentBackend
from docling.backend.msexcel_backend import MsExcelDocumentBackend
from docling.backend.mspowerpoint_backend import MsPowerpointDocumentBackend

from docling.pipeline.simple_pipeline import SimplePipeline
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
import os

from pypdf import PdfReader
from utils import extract_text_from_mhtml,iter_zip_entries,get_config,get_esclient,index_exists
EXCLUDED_DIRS =get_config("docling")["EXCLUDED_DIRS"]
IGNORED_EXT=get_config("docling")["IGNORED_EXT"]
UNKNOWN_EXT=get_config("docling")["UNKNOWN_EXT"]
NOCHUNK_EXT=get_config("docling")["NOCHUNK_EXT"]

os.environ["DOCLING_SERVE_ALLOW_EXTERNAL_PLUGINS"] = "true"
EMBED_MODEL_ID = get_config("semantic_model")["transformer_model_name"]
allowed_formats=[
            InputFormat.PDF,
            InputFormat.IMAGE,
            InputFormat.DOCX,
            InputFormat.HTML,
            InputFormat.PPTX,
            InputFormat.ASCIIDOC,
            InputFormat.CSV,
            InputFormat.MD,
            InputFormat.XLSX,
            "py","rtf",
            InputFormat.XML_USPTO
            
        ]
#allowed_formats=[InputFormat.CSV]
doc_converter = DocumentConverter(  # all of the below is optional, has internal defaults.
         allowed_formats=allowed_formats, # whitelist formats, non-matching files are ignored.
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_cls=StandardPdfPipeline, backend=PyPdfiumDocumentBackend
            ),
            InputFormat.DOCX: WordFormatOption(
                pipeline_cls=SimplePipeline  # or set a backend, e.g., MsWordDocumentBackend
                # If you change the backend, remember to import it, e.g.:
                #   from docling.backend.msword_backend import MsWordDocumentBackend
            ),
            InputFormat.CSV: CsvFormatOption(
                pipeline_cls= SimplePipeline
            ),
             InputFormat.ASCIIDOC: AsciiDocFormatOption(
                pipeline_cls= SimplePipeline
            ),
              "rtf": AsciiDocFormatOption(
                pipeline_cls= SimplePipeline
            ),
               "py": AsciiDocFormatOption(
                pipeline_cls= SimplePipeline
            )
        },
    )
def chunkit(res:ConversionResult,filename:str,doc:dict):
    filesplit=filename.split(":")
    if len(filesplit)==2:
        filename1=Path(filesplit[1])
    else:
        filename1=Path(filename)    
    ext=filename1.suffix.lower()
    #print(ext,filename1,filename)
    doc["fulltext"]=res.document.export_to_markdown()
    doc["path"]={"real":filename}
    doc["file"]={"extension":ext}
    EsClient.index(index=args.index_name, body=doc)
    print(f"ingested {filename}")
    print(f"chunking {filename}")
    chunk_iter = chunker.chunk(dl_doc=res.document)
    
    #for i, chunk in enumerate(chunk_iter):
        # print(f"=== {i} ===")
        # print(f"chunk.text:\n{f'{chunk.text[:300]}…'!r}")

        # enriched_text = chunker.contextualize(chunk=chunk)
        # print(f"chunker.contextualize(chunk):\n{f'{enriched_text[:300]}…'!r}")

        # print()
    chunks=[]
    for i, chunk in enumerate(chunk_iter):
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
    chunks.append(chunk_dict)
    index_chunks(chunks)         
def ingest_text(res,filename):
    doc={}
    filesplit=filename.split(":")
    if len(filesplit)==2:
        filename1=Path(filesplit[1])
    else:
        filename1=Path(filename)    
    ext=filename1.suffix.lower()
    
    doc["fulltext"]=res
    doc["path"]={"real":filename}
    doc["file"]={"extension":ext}
    EsClient.index(index=args.index_name, body=doc)
    print(f"ingested {filename}")
    pass
def index_chunks(chunks):
    pass         
def extensionDealer(f:str, label:str=""):
    if label=="": label=str(f)
    if f.suffix.lower()==".rtf":
        print("converting rtf yo md")
        markdown_text = pypandoc.convert_file(f, to='markdown', format='rtf')
        res=doc_converter.convert_string(markdown_text,InputFormat.MD,f.name)
        
    elif f.suffix.lower() in [".csv",".py",".js",".sh",".json"] : #do no chunk       
        file_content=""
        with open(f, 'r', encoding='utf-8') as file:
            file_content = file.read()
            #res=doc_converter.convert_string(file_content,InputFormat.MD,f.name)  
        ingest_text(file_content,label) 
        return
    elif f.suffix.lower() in [".txt",".msg"] : #convert as string
        file_content=""
        with open(f, 'r', encoding='utf-8') as file:
            file_content = file.read()
        res=doc_converter.convert_string(file_content,InputFormat.MD,f.name)  
    elif f.suffix.lower()==".mhtml":
        file_content=extract_text_from_mhtml(f)  
        res=doc_converter.convert_string(file_content,InputFormat.MD,f.name) 
    elif f.suffix.lower() in [".exe",".com",".dll",".so",".svg"]: #ignore
        print(f"ignore {f}")
        return 
    elif  f.suffix.lower()==".zip":
        print(f"Browsing zip {f}-->")
        for internal_path, data in iter_zip_entries(f): 
            suffix = Path(internal_path).suffix or ""
            if any(part in EXCLUDED_DIRS for part in internal_path.parts):
                continue
           
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
                    tmp.write(data)
                    tmp.flush()
                    
                    extensionDealer(Path(tmp.name),str(f)+":"+str(internal_path))
        return            
    elif   f.suffix.lower() in ["",".sample"]: #unknown
        try:
            file_content=""
            with open(f, 'r', encoding='utf-8') as file:
                file_content = file.read()
            res=doc_converter.convert_string(file_content,InputFormat.MD,f.name)
        except:
            print(f"no file extension coudnt be read {f}")
            return        
                     
    else:  
        try: 
            res=doc_converter.convert(f)
        except UnicodeDecodeError as e:
            print(f"unicode error on {f}")
            return
    doc=meta_it(f)    
    chunkit(res,label,doc)
def meta_it(file_path):
    doc={}
    if file_path.suffix.lower()==".pdf":
        reader = PdfReader(file_path)
        meta = reader.metadata
        doc["metadata"] = meta
        if meta.get("/Title"):
            doc["title"] = meta.get("/Title")  
    elif file_path.suffix.lower()==".pptx":
    # Load presentation and access core_properties
        prs = Presentation(file_path)
        props = prs.core_properties
# Convert to dict
        metadata = {
            "author": props.author,
            "category": props.category,
            "comments": props.comments,
            "content_status": props.content_status,
            "created": props.created.isoformat() if props.created else None,
            "identifier": props.identifier,
            "keywords": props.keywords,
            "language": props.language,
            "last_modified_by": props.last_modified_by,
            "last_printed": props.last_printed.isoformat() if props.last_printed else None,
            "modified": props.modified.isoformat() if props.modified else None,
            "revision": props.revision,
            "subject": props.subject,
            "title": props.title,
            "version": props.version,
        }

        doc["metadata"] = metadata
        #print(f"Author: {prop.author}, Subject: {prop.subject}")    
    elif file_path.suffix.lower()==".xlsx": 
    # Load workbook and access properties
        wb = load_workbook(file_path)
        
        props = wb.properties

# Convert to dict
        props_dict = {
            "title": props.title,
            "subject": props.subject,
            "creator": props.creator,
            "keywords": props.keywords,
            "description": props.description,
            "lastModifiedBy": props.lastModifiedBy,
            "revision": props.revision,
            "created": props.created.isoformat() if props.created else None,
            "modified": props.modified.isoformat() if props.modified else None,
            "category": props.category,
            "contentStatus": props.contentStatus,
            "identifier": props.identifier,
            "language": props.language,
            "version": props.version
        }
        doc["metadata"] = props_dict
        #print(f"Creator: {prop.creator}, Title: {prop.title}")
    elif file_path.suffix.lower()==".docx":
    # Load document and access core_properties
        doc1 = docx.Document(file_path)
        props = doc1.core_properties
        metadata = {
            "author": props.author,
            "category": props.category,
            "comments": props.comments,
            "content_status": props.content_status,
            "created": props.created.isoformat() if props.created else None,
            "identifier": props.identifier,
            "keywords": props.keywords,
            "language": props.language,
            "last_modified_by": props.last_modified_by,
            "last_printed": props.last_printed.isoformat() if props.last_printed else None,
            "modified": props.modified.isoformat() if props.modified else None,
            "revision": props.revision,
            "subject": props.subject,
            "title": props.title,
            "version": props.version,
        }
        doc["metadata"] = metadata
        #print(f"Author: {prop.author}, Created: {prop.created}")
    
      
    return doc      
if __name__ == "__main__":
    description="Index all files in a directory to an index using FSCrawler"
    parser = argparse.ArgumentParser(description=description)
    
    parser.add_argument("index_name", help="Name of the Elastic index to create")
    parser.add_argument("path", help="path to the indexed directory")
    parser.add_argument("--new", help="Delete index name and create new one", action="store_true")
    args = parser.parse_args()
    EsClient=get_esclient()
    exts = {".pdf", ".docx"}
    folder=Path(args.path)
    files = list(folder.rglob("*.zip"))
    chunker = HybridChunker(tokenizer=EMBED_MODEL_ID,max_tokens=350)
    #files = [p for p in files if p.suffix.lower() in exts]
    #for file in files:loader = DoclingLoader(file_path=file,chunker=chunker)
    with open("doclingMap.json") as f:
        mappings = json.load(f)
    if args.new:
        print(f"Deleting index {args.index_name} if exists")
        EsClient.indices.delete(index=args.index_name, ignore=[400, 404])
        EsClient.indices.create(index=args.index_name,body=mappings)
    if not index_exists(EsClient,args.index_name):
        print(f"Index {args.index_name} does not exist, creating")
        EsClient.indices.create(index=args.index_name,body=mappings)   
    
    for file in files:
        extensionDealer(file)
   