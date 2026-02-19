from collections import defaultdict
import datetime
from typing import Dict, List
from sentence_transformers import SentenceTransformer
from __init__ import  EmbeddingModel,EsClient,CONFIG
import json
import ollama
import argostranslate.translate
import argostranslate.package

SYSTEM_PROMPT = """
You are an expert assistant answering questions using a retrieval-augmented generation (RAG) system.

You are given:
• A user question
• A set of retrieved document chunks
• Each chunk contains:
  - chunk_number: integer
  - text: extracted from PDF documents
  - pages: the page numbers where this text appears

Your task:
1. Answer the user question using ONLY the provided chunks.
2. If the answer cannot be found in the provided chunks, say:
   "The provided documents do not contain enough information to answer this question."


Answering rules:
• Prefer factual accuracy over completeness.
• Do NOT use prior knowledge or make assumptions beyond the provided text.
• Do NOT invent facts, definitions, or explanations.
• Do NOT merge unrelated information from different chunks unless they clearly refer to the same concept.
• When multiple chunks are relevant, synthesize them carefully and consistently.

Citation rules:
• Every factual claim MUST be supported by at least one chunk.
• Cite sources inline using this format:
  [chunk_number]
  Example: [1]
• If multiple documents support a statement, list each separately.


Style guidelines:
• Be concise and precise.
• Use clear, professional language.
• Prefer bullet points for multi-step explanations.
• Do NOT mention embeddings, vector search, Elasticsearch, or retrieval mechanics.
• Do NOT reference “chunks” explicitly in the final answer.
• Do NOT include irrelevant information.



When summarizing:
• Preserve technical meaning.
• Do NOT oversimplify.
• Do NOT remove important conditions or caveats.

When listing procedures or requirements:
• Follow the exact order and wording implied by the source text.
• Do NOT add steps that are not explicitly stated.

If the user asks for:
• Opinions → respond only if opinions are explicitly present in the documents.
• Comparisons → respond only if both sides are described in the documents.
• Causes or implications → respond only if directly stated or clearly implied in the documents.

Your goal:
Produce a faithful, well-cited answer grounded strictly in the retrieved document content.
"""
def get_installed_pairs():
    pairs = []
    installed_languages = argostranslate.translate.get_installed_languages()
    print(installed_languages)
    for from_lang in installed_languages:
        for to_lang in from_lang.translations_to:
            if to_lang.from_lang.code==to_lang.to_lang.code:
                continue
            pairs.append({
                "from_code": to_lang.from_lang.code,
                "from_name": to_lang.from_lang.name,
                "to_code": to_lang.to_lang.code,
                "to_name": to_lang.to_lang.name,
            })
    return pairs

def fetch_rows(limit=1000):
    resp = EsClient.search(
        index=CONFIG["index"]+"_logs",
        size=limit,
        query={"match_all": {}},
        sort=[{"date": {"order": "desc"}}]
    )

    rows = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        rows.append({
            "index": src.get("index"),
            "query": src.get("query"),
            "extensions": ", ".join(src.get("extensions", [])),
            "search_type": src.get("search_type"),
            "date": src.get("date"),
        })

    return rows
def insertLog(index:str,query:str,extensions:list,search_type):
    doc={"index":index,
         "query":query,
         "extensions":extensions,
         "search_type":search_type,
         "date": datetime.datetime.now().isoformat()}
    EsClient.index(index=f"{index}_logs", document=doc)
def get_config(key: str):
    with open("config.json") as f:
        d = json.load(f)
        keys = key.split(".")
        try:
            for k in keys:
                d = d[k]
            return d
        except KeyError:
            raise KeyError(f"Unknown config key {key}")

def get_response( system_prompt: str, model_name: str,  input_text: str):
    ajr = ""
    chunk= ollama.chat(model=model_name, messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": input_text}
    ],
    options={
        'temperature': 1.0,
        'top_p': 0.95,
        'top_k': 40,
        'num_predict': 8192,
    })
    if chunk["message"]["content"]:
            ajr += chunk["message"]["content"]

    return ajr

def search_chunks_knn(es_client, chunk_index: str, nchunks: int, model: SentenceTransformer, query: str, document_id: str = None) -> List[Dict]:
    query_vector = ollama.embed(model="qwen3-embedding:8b", input=[query], dimensions=1024).embeddings[0]
#     query_vector =   EmbeddingModel.encode(
#     query,
#     batch_size=32,
#     show_progress_bar=True,
#     normalize_embeddings=False
# )
    # Base query for KNN search
    knn_query = {
        "field": "embedding",
        "query_vector": query_vector,
        "k": nchunks,
        "num_candidates": nchunks * 2
    }

    # Add a filter for document_id if provided
    base_query = {
        "knn": knn_query,
        "_source": ["doc_id", "text", "pages", "doc_index"]
    }

    if document_id:
        # Handle both single ID (string) and multiple IDs (list)
        if isinstance(document_id, list):
            base_query["query"] = {
                "terms": {
                    "doc_id": document_id
                }
            }
        else:
            base_query["query"] = {
                "term": {
                    "doc_id": document_id
                }
            }

    response = es_client.search(
        index=chunk_index,
        body=base_query
    )

    return response["hits"]["hits"]


def aggregate_max_score(chunk_hits: List[Dict]) -> Dict[str, float]:
    doc_scores = {}

    for hit in chunk_hits:
        doc_id = hit["_source"]["doc_id"]
        doc_index = hit["_index"].replace("_chunks", "")    
        score = hit["_score"]
        score_name=f"{doc_index}/{doc_id}"
        hit["_source"]["id"]=hit["_id"]

        #print(f"Doc {score_name} score: {score}")
        #if doc_id not in doc_scores or score > doc_scores[doc_id]:
        #    doc_scores[f"{score_name}"] = score
        if score_name not in doc_scores :
            doc_scores[f"{score_name}"]={"score": score}
            doc_scores[f"{score_name}"]["s_chunks"]=[hit["_source"]]
            continue
        if score > doc_scores[score_name]["score"]:
                doc_scores[f"{score_name}"]={"score": score}
        doc_scores[f"{score_name}"]["s_chunks"].append(hit["_source"])


    return doc_scores


def fetch_documents(es_client, doc_scores: Dict[str, float], limit: int, selected_extensions: List[str], query: str) -> List[Dict]:
    ranked = sorted(
        doc_scores.items(),
        key=lambda x: x[1]["score"],
        reverse=True
    )

    filtered_documents = []
    highlight_query=build_query(query, "multi_match" )
    highlight_query["multi_match"]["analyzer"]="stop"
    fields={field: {"highlight_query": highlight_query} for field in get_config("highlight_fields")}
    highlight={
            'fields': fields,
            'pre_tags': ['<em class="highlight">'],
            'post_tags': ['</em>']
            
    }
    
    for did, v in ranked:
        doc_index, doc_id = did.split("/")
        base_query={
     "query": {
    "ids": {
      "values": [doc_id]
    }
  }}
        #document = es_client.get(index=doc_index, id=doc_id)
        result = es_client.search(index=doc_index, body=base_query,size=1,highlight=highlight )
        document = result["hits"]["hits"][0]
        document["s_chunks"]=v["s_chunks"]
        if len(selected_extensions) == 0 or document["_source"].get("file", {}).get("extension") in selected_extensions:
            filtered_documents.append(document)

    return filtered_documents[:limit]


def semantic_search_documents(es_client, chunk_index, nchunks, ndocs, model, query: str, selected_extensions: List[str]) -> List[Dict]:
    #print("Query:", query, "Extensions:", selected_extensions, "ndocs:", ndocs, "chunk_index:", chunk_index, "nchunks:", nchunks)
    chunk_hits = search_chunks_knn(es_client, chunk_index, nchunks, model, query)
    doc_scores = aggregate_max_score(chunk_hits)
    return fetch_documents(es_client, doc_scores, ndocs, selected_extensions,query)

def lexical_search_documents(es_client, query: str, allowed_extensions: List[str]):
    base_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": query,
                        "fields": get_config("search_fields")
                    }
                }
            ]
        }
    }
    }
    fields={field: {"highlight_query": {"match": {field: query}}} for field in get_config("highlight_fields")}
    highlight={
            'fields': fields,
            'pre_tags': ['<em class="highlight">'],
            'post_tags': ['</em>']
            
    }

    # Add filter for allowed extensions if the list is not empty
    if len(allowed_extensions) > 0:
        base_query['query']['bool']['filter'] = [
            {'terms': {'file.extension': allowed_extensions}}
        ]

    result = es_client.search(index=get_config("index"), body=base_query,size=1000,highlight=highlight )

    # In case the highlight failed, try to run query without highlighting
    if len(result["hits"]["hits"]) == 0:
        #base_query.pop('highlight', None)  # Remove highlighting for the fallback query
        result = es_client.search(index=get_config("index"), body=base_query)
    #print("Lexical search found", result["hits"]["hits"][0])
    return result["hits"]["hits"]

def buildLog(index_name):
    es=EsClient
    index_name = index_name+"_logs"

    mapping = {
        "mappings": {
            "properties": {
                "query": {"type": "text"},
                "date": {"type": "date"},
                "search_type": {"type": "keyword"},
                "extensions": {"type": "keyword"},
                "index": {"type": "keyword"}
            }
        }
    }

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)

def orderGroups(hits):
    groups = defaultdict(list)
    singletons = []

    for hit in hits:
        name = hit.hit["_source"].get("data", {}).get("name")
        if name:
            groups[name].append(hit)
        else:
            # items without name are their own group
            singletons.append([hit])

    # Sort each group descending by _score
    for group_hits in groups.values():
        group_hits.sort(key=lambda x: x.hit["_score"], reverse=True)

    # Combine groups and singletons
    all_groups = list(groups.values()) + singletons

    # Sort all groups by the group's max _score
    all_groups.sort(key=lambda g: g[0].hit["_score"], reverse=True)
    #for g in all_groups:
    #    print([h.hit["_score"] for h in g])
    return all_groups

def similar_documents(es_client, document_id: str, document_index: str, ndocs: int,similar_fields:list) -> List[Dict]:
    """
    Search for documents similar to a given document using more_like_this query.
    
    Args:
        es_client: Elasticsearch client
        document_id: ID of the reference document
        document_index: Index name containing the document
        ndocs: Maximum number of documents to return
        selected_extensions: List of file extensions to filter by
    
    Returns:
        List of similar documents with highlighting
    """
    base_query = {
        "query": {
            "more_like_this": {
                "like": [{"_index": document_index, "_id": document_id}],
                "min_term_freq": 1,
                "max_query_terms": 25,
                "fields": similar_fields
            }
        }
    }
    
    print(base_query)
    
    result = es_client.search(index=document_index, body=base_query, size=ndocs)
    
    return result["hits"]["hits"]

def build_pagespan_map(chunks):
    result = {}

    for chunk in chunks:
        chunk_id = chunk.get("id")
        text = chunk.get("text", "")
        #print(chunk)
        # collect unique page numbers from docling
        # pages = {
        #     prov["page_no"]
        #     for item in chunk.get("items", [])
        #     for prov in item.get("prov", [])
        # }
        pages=chunk["pages"]
        if not pages or not chunk_id:
            continue

        pages_tuple = tuple(sorted(pages))  # hashable

        result[chunk_id] = [pages_tuple, text]
        #print(result)
    return result


def build_rag_prompt_messages(
    chunks: List[Dict[str, str]],
    user_query: str,
) -> List[Dict[str, str]]:
    """
    Builds the messages payload for a RAG-enabled LLM call.

    Args:
        system_prompt: The RAG system prompt (string).
        user_query: The user's question.
        chunks: Retrieved chunks, each with fields:
            - doc_id
            - text
            - pages (list[int])

    Returns:
        messages: List of messages suitable for chat-based LLM APIs.
    """

    # Defensive truncation
    selected_chunks = chunks

    context_blocks = []
    for i, chunk in enumerate(selected_chunks, 1):
        chunk = chunk["_source"]
        pages = ", ".join(str(p) for p in sorted(chunk["pages"])) if "pages" in chunk else ""

        block = (
            f"CHUNK ID: {i}\n"
            f"CONTENT:\n{chunk['text']}"
        )
        context_blocks.append(block)

    context_text = "\n\n---\n\n".join(context_blocks)

    user_message = (
        "Answer the question using the following source documents.\n\n"
        f"{context_text}\n\n"
        f"QUESTION:\n{user_query}"
    )

    return user_message



def rag_query(es_client, chunk_index, nchunks, embedding_model, prompt: str, document_id: str):
    
    chunks = search_chunks_knn(es_client, chunk_index, nchunks, embedding_model, prompt, document_id=document_id)
    prompt_with_chunks = build_rag_prompt_messages(chunks, prompt)

    # client = genai.Client(
    #     api_key=get_config("chat_settings.key")
    # )

    model = get_config("chat_settings.model_name")

    #config = default_model_config(SYSTEM_PROMPT)

    return get_response( SYSTEM_PROMPT, model,  prompt_with_chunks), chunks


def chunks_to_sources(es_client, document_index, chunks):
    sources = []
    for c in chunks:
        doc_id = c["_source"]["doc_id"]
        doc_index = c["_source"].get("doc_index", "pdfs")
        document = es_client.get(index=document_index, id=doc_id)["_source"]
        sources.append({"path": document["path"]["real"], "docid": doc_id,
                        "docindex": doc_index, "pages": c["_source"]["pages"], "chunkid": c["_id"]})
    return sources


def get_available_extensions(es_client):
    result = es_client.search(
        body={
            "size": 0,
            "aggs": {
                "unique_extensions": {
                    "terms": {
                        "field": "file.extension",
                        "size": 100
                    }
                }
            }
        }
    )


    return [bucket["key"] for bucket in result["aggregations"]["unique_extensions"]["buckets"]]
def build_query(query_text, query_type):
    global model
    fields = get_config("search_fields")
    # if model is None and query_type == "Semantic":
    #     print("Model not loaded yet, using default match query.")
    #     query_type = "multi_match"
    if query_type == "fuzzy":
        return {
            "multi_match": {
                "query": query_text,
                "fields": fields,
                "fuzziness": "AUTO"
            }
        }

    elif query_type == "phrase":
        return {
            "multi_match": {
                "query": query_text,
                "fields": fields,
                "type": "phrase"
            }
        }
    elif query_type == "semantic":
        #print(f"Building semantic query {query_text} {CONFIG['semantic_model']['content_embedding_field']} {CONFIG['semantic_model']['filename_embedding_field']}")
        query_vector = model.encode(query_text).tolist()
        #print("Query vector :", query_vector)
        return {
            "script_score": {
               "query": {
            "exists": {
              "field": "has"
            }
          },
                "script": {
                    "source": """double s1=cosineSimilarity(params.query_vector, '{}')+1 ; 
                    double s2=cosineSimilarity(params.query_vector, '{}')+1 ;
                     return Math.max(s1, s2);""".format(get_config("semantic_model")["content_embedding_field"], 
                                                        get_config("semantic_model")["filename_embedding_field"]),
                    "params": {"query_vector": query_vector}
                }
            }
        }
    elif query_type == "function_score":
        query_vector = model.encode(query_text).tolist()
        return {
            "function_score": {
                "query": {
                    "multi_match": {
                        "query": query_text,
                        "fields": fields,
                        
                    }
                },
                "boost_mode": "multiply",
                "functions": [
                    {
            "script_score": {
              
                "script": {
                    "source": """double s1=cosineSimilarity(params.query_vector, '{}')+1 ; 
                    double s2=cosineSimilarity(params.query_vector, '{}')+1 ;
                     return Math.max(s1, s2);""".format(get_config("semantic_model")["content_embedding_field"], 
                                                        get_config("semantic_model")["filename_embedding_field"]),
                    "params": {"query_vector": query_vector}
                }
            }
        }
                ]
            }
        }    
    elif query_type == "wildcard":
        # Wildcard doesn't support multi_match — build OR terms per field
        should_clauses = [{"wildcard": {f: f"{query_text}*"}} for f in fields]
        return {"bool": {"should": should_clauses}}

    elif query_type == "regexp":
        should_clauses = [{"regexp": {f: query_text}} for f in fields]
        return {"bool": {"should": should_clauses}}

    elif query_type == "more_like_this":
        print("Building more_like_this query with fields:", fields)
        return {
            "more_like_this": {
                "fields": fields,
                "like": query_text,
                "min_term_freq": 1,
                "max_query_terms": 25
            }
        }

    elif query_type == "query_string":
        return {
            "query_string": {
                "query": query_text,
                "fields": fields
            }
        }

    else:
        # Default: match on all fields
        return {
            "multi_match": {
                "query": query_text,
                "fields": fields
            }
        }
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from __init__ import EsClient, EmbeddingModel

    print(get_available_extensions(es_client=EsClient))
    #sys.exit()

    response, chunks = rag_query(
        es_client=EsClient,
        chunk_index="pdfs_chunks",
        embedding_model=EmbeddingModel,
        nchunks=5,
        prompt="what are the biggest challenges in training teachers to teach foreign languages?"
    )
    print(response)
    print("SOURCES:")
    for i, c in enumerate(chunks):
        doc_id = c["_source"]["doc_id"]
        document = EsClient.get(index="pdfs", id=doc_id)["_source"]
        c = c["_source"]
        print(i + 1, document["path"]["real"], c["pages"])