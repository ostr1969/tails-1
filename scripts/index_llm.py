# import utils
from pydantic import BaseModel, ValidationError
import ollama
import argparse
import time
from elasticsearch import helpers
from sentence_transformers import SentenceTransformer

# CONTENT_FIELD = utils.get_config("semantic_model")["content_field"]
# FILENAME_FIELD = utils.get_config("semantic_model")["filename_field"]
# CONTENT_EMBEDDING = utils.get_config("semantic_model")["content_embedding_field"]
# FILENAME_EMBEDDING = utils.get_config("semantic_model")["filename_embedding_field"]
# MODEL_NAME = utils.get_config("semantic_model")["model_name"]

import re
from typing import List, Dict, Any


def approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def layout_to_paragraphs(layout: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Groups layout into paragraphs.
    Returns a list of:
        {
            "text": <paragraph text>,
            "pages": set([page numbers])
        }
    """

    paragraphs = []
    buffer = []
    pages = set()

    def flush():
        nonlocal buffer
        nonlocal pages
        if buffer:
            paragraphs.append({
                "text": "\n".join(buffer).strip(),
                "pages": set(pages),
            })
            buffer = []
            pages = set()

    for sec in layout:
        t = sec["type"].lower()
        text = sec["text"].strip()
        page = sec["page_number"]
        if t in ("title", "section header"):
            flush()
            buffer.append(text)
            pages.add(page)

        elif t == "text":
            buffer.append(text)
            pages.add(page)

        elif t in ("list item", "caption", "footnote"):
            buffer.append(text)
            pages.add(page)

    flush()
    return paragraphs


def split_long_paragraph(paragraph: Dict[str, Any], max_tokens: int, overlap: int) -> List[Dict[str, Any]]:
    """
    Splits a long paragraph into smaller ones while tracking pages.
    """
    text = paragraph["text"]
    pages = paragraph["pages"]

    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s.strip() for s in sentences if s.strip()]

    chunks = []
    current = []
    current_tokens = 0

    for sentence in sentences:
        st = approx_tokens(sentence)

        if current_tokens + st > max_tokens and current:
            # finalize
            chunks.append({
                "text": " ".join(current),
                "pages": set(pages),
            })

            # overlap
            if overlap > 0:
                overlap_chars = overlap * 4
                prev_text = chunks[-1]["text"]
                overlap_text = prev_text[-overlap_chars:].split()
                current = [" ".join(overlap_text)]
                current_tokens = approx_tokens(current[0])
            else:
                current = []
                current_tokens = 0

        current.append(sentence)
        current_tokens += st

    if current:
        chunks.append({
            "text": " ".join(current),
            "pages": set(pages),
        })

    return chunks


def chunk_paragraphs(
    paragraphs: List[Dict[str, Any]],
    max_tokens: int = 350,
    overlap: int = 50
) -> List[Dict[str, Any]]:

    chunks = []
    current_chunk = []
    current_pages = set()

    for para in paragraphs:
        ptext = para["text"]
        ppages = para["pages"]
        ptokens = approx_tokens(ptext)

        if ptokens > max_tokens:
            # finish pending chunk first
            if current_chunk:
                chunks.append({
                    "text": "\n\n".join(current_chunk),
                    "pages": set(current_pages),
                })
                current_chunk = []
                current_pages = set()

            # split long paragraph
            long_subchunks = split_long_paragraph(para, max_tokens, overlap)
            chunks.extend(long_subchunks)
            continue

        chunks.append({
            "text": ptext,
            "pages": set(ppages)
        })

    # final chunk
    if current_chunk:
        chunks.append({
            "text": "\n\n".join(current_chunk),
            "pages": set(current_pages)
        })

    return chunks


def chunk_sections(sections: List[Dict[str, Any]], max_tokens=350, overlap=50) -> List[Dict[str, Any]]:
    paragraphs = layout_to_paragraphs(sections)
    return chunk_paragraphs(paragraphs, max_tokens=max_tokens, overlap=overlap)

def chunk_text(text: str, max_tokens: int = 350, overlap: int = 50) -> List[Dict[str, Any]]:
    """
    Splits a long text string into chunks with overlap.
    Returns a list of chunks with their token counts.
    """
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s.strip() for s in sentences if s.strip()]
    
    chunks = []
    current = []
    current_tokens = 0
    
    for sentence in sentences:
        st = approx_tokens(sentence)
        
        if current_tokens + st > max_tokens and current:
            chunk_text = " ".join(current)
            chunks.append({"text": chunk_text, "pages": []})
            
            if overlap > 0:
                overlap_chars = overlap * 4
                overlap_text = chunk_text[-overlap_chars:].split()
                current = [" ".join(overlap_text)]
                current_tokens = approx_tokens(current[0])
            else:
                current = []
                current_tokens = 0
        
        current.append(sentence)
        current_tokens += st
    
    if current:
        chunks.append({"text": " ".join(current), "pages": []})
    
    return chunks

class Chunk(BaseModel):
    text: str

def clean_chunks(chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter chunks that pass Pydantic validation"""
    valid_chunks = []
    invalid_count = 0
    
    for i, chunk_data in enumerate(chunks):
        try:
            # Validate with Pydantic
            chunk = Chunk(**chunk_data)
            # Convert back to dict if valid
            valid_chunks.append(chunk_data)
        except ValidationError as e:
            invalid_count += 1
            continue
    
    print(f"Cleaned {len(chunks)} chunks: {len(valid_chunks)} valid, {invalid_count} invalid")
    return valid_chunks

def embed_document(es_client, doc: dict, model: str, index_name: str, chunk_size: int, chunk_overlap: int):
    layout = doc["_source"].get("huridocs", {}).get("layout")
    if layout:
        chunks = chunk_sections(layout, max_tokens=chunk_size, overlap=chunk_overlap)
    else:
        chunks = chunk_text(doc["_source"].get("content", ""), max_tokens=chunk_size, overlap=chunk_overlap)
    chunks = clean_chunks(chunks)
    doc_id = doc["_id"]
    actions = []
    match_query = {
    "query": {
        "match": {
            "doc_id": doc_id
        }
    }
}
    
    if not args.restart and existindex:
        if es.count(index=index_name + "_chunks", body=match_query)["count"] > 0:
                print(f"{doc_id} already exists, skipping...")
                return  # Skip if chunk index already exists
            
    embeddings = ollama.embed(model=model, input=[c["text"] for c in chunks], dimensions=1024).embeddings
    for embedding, chunk in zip(embeddings, chunks):
    # for chunk in chunks:
        text = chunk["text"]
        # embedding = model.encode(text).tolist()
        # counter += 1 
        # print('embedding', counter, 'chunk')
        # embedding = list(ollama.embed(model="qwen3-embedding:8b", input=text, dimensions=1024).embeddings[0])
  
        action = {
            "_op_type": "index",
            "_index": index_name + "_chunks",
            "_id": f"{doc_id}_{chunks.index(chunk)}",  # Unique ID for each chunk
            "_source": {
                "doc_id": doc_id,
                "doc_index": doc["_index"],
                "embedding_model": model,
                "text": text,
                "embedding": embedding,
                "pages": list(chunk["pages"]),
            },
        }
        actions.append(action)

    # Send the actions to Elasticsearch in bulk
    helpers.bulk(es_client, actions)



if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import utils

    description="reIndex semantic from Elastic index."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("index_name", type=str, help="name of index need to create chunk data")
    parser.add_argument("--chunk_size", type=int, help="set maximal number of tokens per chunk. default=350", default=350)
    parser.add_argument("--chunk_overlap", type=int, help="set number of overlapping tokens between chunks. default=50", default=50)
    parser.add_argument("--restart", action="store_true", help="re-index all documents even if already indexed")
    args = parser.parse_args()
    # --- INITIALIZE ---
    #print(f"Loading embedding model...")
    #model = SentenceTransformer(utils.get_config("semantic_model")["model_name"])
    model=utils.get_config("semantic_model")["ollama_embedding_model"]
    #print("Model loaded ✅")
    es = utils.get_esclient()
    existindex=es.indices.exists(index=args.index_name + "_chunks")
    
    nfiles = utils.count_files_with_extension(es, args.index_name, "pdf")
    print(f"Start embedding of {nfiles}...")
    for i, hit in enumerate(utils.search_by_extension(es, args.index_name, "pdf")):
        print(f"embedding document {i + 1} out of {nfiles} (id={hit['_id']})")
        start = time.time()
        try:
            embed_document(es, hit, model, args.index_name, args.chunk_size, args.chunk_overlap)
            print("done! took", round(time.time() - start, 2), "seconds")
        except:
            print("FAILED INDEXING FOR DOCUMENT!")
            
