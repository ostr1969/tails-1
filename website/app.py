from shutil import copyfile
from flask import render_template, request, send_file, jsonify
from flask import request

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
import utils
import fscrawlerUtils as fsutils
from SearchHit import hits_from_resutls
from __init__ import app, EsClient, CONFIG, EmbeddingModel

session_data = {}

@app.route('/', methods=['GET', 'POST'])
def search():
    # Pagination
    if request.method == "GET":
        session_data.clear()
    page = int(request.args.get('page', 1))
    start = (page - 1) * CONFIG["results_per_page"]
    end = start + CONFIG["results_per_page"]

    query = request.form.get('query',  request.args.get('query', ''))
    semantic_search = request.form.get('search_mode', 'normal') != 'normal'
    semantic_search = True if semantic_search else False
    selected_extensions = request.form.getlist('file_extensions')
    session_data["selected_extensions"] = selected_extensions    
    session_data["query"] = query
    session_data["semantic_search"] = semantic_search
    
    print(session_data)
    
     # If no query is provided, return empty results
     
    #print("query:", query, "semantic_search:", semantic_search, "selected_extensions:", selected_extensions)
    if len(query) == 0:
        return render_template('index.html')

    # Perform the search based on the selected type
    if semantic_search:
        result = utils.semantic_search_documents(
            es_client=EsClient,
            chunk_index=CONFIG["semantic_search"]["chunk_index"],
            nchunks=CONFIG["semantic_search"]["nchunks"],
            ndocs=CONFIG["semantic_search"]["ndocs"],
            model=EmbeddingModel,
            query=query,
            selected_extensions=selected_extensions
        )
    else:
        result = utils.lexical_search_documents(EsClient, query, selected_extensions)


    # Extract relevant information from the result
    hits = hits_from_resutls(result)
    
    total_hits = len(hits)
    hits = hits[start:end]

    return render_template(
        'search.html', 
        hits=hits, 
        total_hits=total_hits, 
        page=page, 
        query=query, 
        results_per_page=CONFIG["results_per_page"],
        semantic_search=semantic_search,
        available_extensions=utils.get_available_extensions(EsClient),
        selected_extensions=selected_extensions
    )
    

@app.route('/json/<index>/<file_id>', methods=['GET'])
def json_view(index: str, file_id: str):
    """endpoint for viewing file"""
    hit = EsClient.get(index=index, id=file_id)
    json_data=hit["_source"]
    if "content" in json_data:
        json_data["content"]=[json_data["content"]]
    return render_template("json.html", json_data=json_data)

@app.route('/more/<file_id>', methods=['POST','GET'])
def more(file_id: str):
    results = utils.similar_documents(EsClient, file_id, CONFIG["index"], utils.get_config("results_per_page"))
    hits = hits_from_resutls(results)
    total_hits = len(hits)
    #print("HITS:", hits[0])
    return render_template('search.html', 
        hits=hits, 
        total_hits=total_hits, 
        page=1, 
        query=f"similar_to:{file_id}",
        results_per_page=CONFIG["results_per_page"],
        semantic_search=False,
        available_extensions=utils.get_available_extensions(EsClient),
        selected_extensions=[])
    

@app.route("/log", methods=["POST"])
def log():
    data = request.json
    print(data["msg"])
    return "", 204

@app.route('/view/<index>/<file_id>', methods=['GET'])
def view(index: str, file_id: str):
    """endpoint for viewing file"""
    hit = EsClient.get(index=index, id=file_id)
    path = hit["_source"]["path"]["real"]
    # change base path in case files were moved after indexing
    for base_path, new_path in CONFIG["base_paths"].items():
        if path.lower().startswith(base_path.lower()):
            path = path.replace(base_path, new_path)
    ext = hit["_source"]["file"]["extension"]
    target = "files/{}.{}".format(file_id, ext)
    copyfile(path, target)
    if ext.lower() in CONFIG["open_file_types"]:
        download = False
    else:
        download = True
    return send_file(target, as_attachment=download)

@app.route('/index', methods=['GET', 'POST'])
def fscraller_index():
    if request.method == "POST":
        name = request.form["jobName"]
        target_dir = request.form["targetDirectory"]
        if fsutils.create_new_job(name):
            fsutils.load_defaults_to_job(name)
            fsutils.edit_job_setting(name, "fs.url", target_dir)
            fsutils.run_job(name)
    CONFIG["index"] = fsutils.get_all_jobs()
    return render_template("fscrawler.html",j=0)

@app.route('/stat', methods=['GET'])
def stat():
    return "OK"
@app.route('/reset', methods=['GET'])
def reset():
    
    return render_template("fscrawler.html",j=1)

@app.route('/_existing_jobs', methods=['GET'])
def existing_jobs_info():
    stats = fsutils.jobs_status()
    return jsonify(stats)

@app.route('/_elasticsearch_statistics', methods=['GET'])
def index_statistics():
    # Get total number of documents
    total_documents = EsClient.count(index=CONFIG["index"])['count']

    # Get total number of documents with content (adjust the query as needed)
    total_documents_with_content = EsClient.count(index=CONFIG["index"], body={"query": {"exists": {"field": "content"}}})['count']

    # Get file extensions distribution
    file_extensions_aggregation = EsClient.search(index=CONFIG["index"], body={
        "size": 0,
        "aggs": {
            "file_extensions": {
                "terms": {
                    "field": "file.extension",
                    "size": 9
                }
            }
        }
    })


    file_extensions_buckets = file_extensions_aggregation['aggregations']['file_extensions']['buckets']
    # addint the "other" count
    file_extensions_buckets.append({"key": "other", "doc_count": file_extensions_aggregation['aggregations']['file_extensions']["sum_other_doc_count"]})
    file_extensions_stats = [{"extension": bucket['key'], "count": bucket['doc_count']} for bucket in file_extensions_buckets]

    # Combine all statistics
    return {
        "total_documents": total_documents,
        "total_documents_with_content": total_documents_with_content,
        "file_extensions": file_extensions_stats
    }

@app.route('/delete_job/<job_name>', methods=['GET'])
def delete_job(job_name: str):
    fsutils.delete_job(job_name)
    return True


@app.route('/chat', methods=['GET', 'POST'])
def chat():
    """
    Endpoint to interact with the Gemini API via a chat interface.
    """
    docids = request.args.getlist('docid')
    
    if request.method == "GET":
        if len(docids) == 0:    
            welcome_message = CONFIG["chat_settings"]["welcome_message"].replace("$SOURCE", "all the documents")
        elif len(docids) == 1:
            welcome_message = CONFIG["chat_settings"]["welcome_message"].replace("$SOURCE", f"docuement id {docids[0]}")
        else:
            welcome_message = CONFIG["chat_settings"]["welcome_message"].replace("$SOURCE", "the search results")
            
        return render_template('chat.html', welcome_message=welcome_message, docids=docids)
    
    
    message = request.json["message"]

    response, chunks = utils.rag_query(EsClient, CONFIG["semantic_search"]["chunk_index"], 5, EmbeddingModel, message, document_id=docids)
    
    sources = utils.chunks_to_sources(EsClient, CONFIG["index"], chunks)
    return jsonify({"response": response, "sources": sources})

    

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
