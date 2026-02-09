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



def embed_document(es_client, doc: dict, index_name: str):
 #   layout = doc["_source"].get("huridocs", {}).get("layout")
    chunks=doc["_source"]["chunks"]

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
    texts = [c["text"] for c in chunks]   
    

    if args.use_ollama:
        model = args.ollama_embedding_model
        print(f"Generating embeddings for {len(texts)} chunks using model {model}...")
        embeddings = ollama.embed(model=model, input=texts, dimensions=1024).embeddings
    else:
        model= args.sentence_transformer_model
        print(f"Generating embeddings for {len(texts)} chunks using model {model}...")
        embeddings = stmodel.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=False
    )
             
    
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
                "items": chunk["items"],
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

    parser.add_argument("--restart", action="store_true", help="re-index all documents even if already indexed")
    parser.add_argument("--use_ollama", action="store_true", help="use ollama for embeddings")
    parser.add_argument("--ollama_embedding_model", type=str, help="ollama embedding model to use", 
                default=utils.get_config("semantic_model")["ollama_embedding_model"])
    parser.add_argument("--sentence_transformer_model", type=str, help="sentence transformer model to use", 
                        default=utils.get_config("semantic_model")["model_name"])
   
    args = parser.parse_args()
    # --- INITIALIZE ---
    #print(f"Loading embedding model...")
    #model = SentenceTransformer(utils.get_config("semantic_model")["model_name"])
    if not args.use_ollama:
        stmodel=SentenceTransformer(args.sentence_transformer_model)
    #print("Model loaded ✅")
    es = utils.get_esclient()
    existindex=es.indices.exists(index=args.index_name + "_chunks")
    if args.restart and existindex:
        print(f"Deleting existing chunk index {args.index_name + '_chunks'}...")
        es.indices.delete(index=args.index_name + "_chunks")
        existindex=False
    nfiles = utils.count_files_with_extension(es, args.index_name, "pdf")
    print(f"Start embedding of {nfiles}...")
    for i, hit in enumerate(utils.search_by_extension(es, args.index_name, "pdf")):
        print(f"embedding document {i + 1} out of {nfiles} (id={hit['_id']})")
        start = time.time()
        embed_document(es, hit, args.index_name)
        print("done! took", round(time.time() - start, 2), "seconds")
        
            
