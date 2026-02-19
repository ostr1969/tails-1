import csv
import io
from shutil import copyfile
from flask import abort, render_template, request, send_file, jsonify
from flask import request,session
from urllib.parse import quote
import argostranslate.translate

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
    
    #print(session_data)
    
     # If no query is provided, return empty results
     
    #print("query:", query, "semantic_search:", semantic_search, "selected_extensions:", selected_extensions)
    if len(query) == 0:
        return render_template('index.html')

    # Perform the search based on the selected type
    if semantic_search:
        result = utils.semantic_search_documents(
            es_client=EsClient,
            chunk_index=CONFIG["index"] + "_chunks",
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
    for hit in hits:
        #hit.hit["r_chunks"]=hit.hit_chunks() # if lexical than its empty , convert s_chunks, to r_chunks which is only the blocks
        if "s_chunks" in hit.hit:
            #sl=[list(k.keys()) for k in hit.hit["r_chunks"]]
            
            #print(hit.chunk_dict)
            pages_set = hit.chunk_dict
            hit.hit["_source"]["chunks_pages"]=list(pages_set)
            #print(list(pages_set))

    
    
    total_hits = len(hits)
    print("found:",total_hits)
    #hits = hits[start:end]
    ghits=utils.orderGroups(hits)
              
    total_hits = len(ghits)
    ghits = ghits[start:end]
    start,end=pagination_window(page,total_hits,3)
    if semantic_search:
        utils.insertLog(CONFIG["index"],query,selected_extensions,"semantic")
    else:
        utils.insertLog(CONFIG["index"],query,selected_extensions,"normal")
    return render_template(
        'search.html', 
        hits=hits, 
        start=start,
        end=end,
        total_hits=total_hits, 
        page=page, 
        query=query, 
        results_per_page=CONFIG["results_per_page"],
        semantic_search=semantic_search,
        available_extensions=utils.get_available_extensions(EsClient),
        selected_extensions=selected_extensions,
        ghits=ghits
    )

@app.route('/more/<file_id>', methods=['POST','GET'])
def more(file_id: str):
    similar_fields=utils.get_config("similar_document_fields")
    results = utils.similar_documents(EsClient, file_id, CONFIG["index"], utils.get_config("results_per_page"),similar_fields)
    hits = hits_from_resutls(results)
    total_hits = len(hits)
    ghits=utils.orderGroups(hits)
    utils.insertLog(CONFIG["index"],"similar_to:"+file_id,[],"similar")          
    total_hits = len(ghits)
    
    #print("HITS:", hits[0])
    return render_template('search.html', 
        hits=hits, 
        total_hits=total_hits, 
        page=1, 
        query=f"similar_to:{file_id}",
        results_per_page=CONFIG["results_per_page"],
        semantic_search=False,
        available_extensions=utils.get_available_extensions(EsClient),
        selected_extensions=[],
        ghits=ghits)

@app.route("/help")
def search_help():
    return render_template("queryhelp.html")
    
@app.route('/filter/<file_id>/<prop>', methods=['POST','GET'])
def filter(file_id: str,prop:str):
    results = utils.similar_documents(EsClient, file_id, CONFIG["index"], utils.get_config("results_per_page"),[prop])
    hits = hits_from_resutls(results)
    total_hits = len(hits)
    ghits=utils.orderGroups(hits)
    utils.insertLog(CONFIG["index"],"similar_to:"+file_id+","+prop,[],"similar")          
    total_hits = len(ghits)
    
    #print("HITS:", hits[0])
    return render_template('search.html', 
        hits=hits, 
        total_hits=total_hits, 
        page=1, 
        query=f"similar_to:{file_id}:{prop}",
        results_per_page=CONFIG["results_per_page"],
        semantic_search=False,
        available_extensions=utils.get_available_extensions(EsClient),
        selected_extensions=[],
        ghits=ghits)    
    
def pagination_window(page, total_pages, window=2):
        start = max(1, page - window)
        end = min(total_pages, page + window)
        return start, end    

@app.route("/log")
def table():
    rows = utils.fetch_rows()
    return render_template("logtable.html", rows=rows)


@app.route("/export/csv")
def export_csv():
    rows = utils.fetch_rows()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["index", "query", "extensions", "search_type", "date"])

    for r in rows:
        writer.writerow([
            r["index"],
            r["query"],
            r["extensions"],
            r["search_type"],
            r["date"]
        ])

    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="pdfs_export.csv"
    )

@app.route('/json/<index>/<file_id>', methods=['GET'])
def json_view(index: str, file_id: str):
    """endpoint for viewing file"""
    hit = EsClient.get(index=index, id=file_id)
    json_data=hit["_source"]
    json_data["id"]=file_id
    if "content" in json_data:
        json_data["content"]=[json_data["content"]]
    return render_template("json.html", json_data=json_data)


    

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
    for base_path, new_path in CONFIG["base_paths"]:
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

@app.route("/argos", methods=["GET", "POST"])
def argostranslate_gui():
    pairs = utils.get_installed_pairs()
    result = ""
    source_text = ""
    selected_pair = ""

    if request.method == "POST":
        source_text = request.form.get("source_text", "")
        selected_pair = request.form.get("pair", "")

        if source_text and selected_pair:
            from_code, to_code = selected_pair.split("->")
            print(from_code,to_code)
            from_lang = next(
                l for l in argostranslate.translate.get_installed_languages()
                if l.code == from_code
            )
            #print(from_lang.translations_to)
            to_lang = next(
                t for t in from_lang.translations_from
                if t.to_lang.code == to_code
            )

            result = to_lang.translate(source_text)

    return render_template(
        "argos.html",
        pairs=pairs,
        result=result,
        source_text=source_text,
        selected_pair=selected_pair
    )


@app.route('/view1/<index>/<file_id>', methods=['GET'])
def view1(index: str, file_id: str):
    """endpoint for viewing file"""
    words = request.args.get("words", "")
    words=",".join(words.split())
    pages = request.args.get("pages", "")
    chunkid = request.args.get("chunkid", "")
    hit = EsClient.get(index=index, id=file_id)
    
    path = hit["_source"]["path"]["real"]
    # change base path in case files were moved after indexing
    for base_path, new_path in CONFIG["base_paths"].items():
        if path.lower().startswith(base_path.lower()):
            path = path.replace(base_path, new_path)
    ext = hit["_source"]["file"]["extension"]
    target = "files/{}.{}".format(file_id, ext)
    copyfile(path, target)
    #print("Serving file:", path, "as", target)
    if ext.lower() in CONFIG["open_file_types"]:
        download = False
    else:
        download = True
    #return send_file(target, as_attachment=download)
    print("Viewing file:", file_id, "pages:", pages, "words:", words,"chunkid:", chunkid)
    viewer = "/static/pdfjs/web/viewer.html"
    return render_template("pdfpager.html", target=quote(target), pages=pages, 
                           viewer=viewer, words=words,chunkid=chunkid,index=index)
@app.route('/chunktext/<index>/<id>')
def getchunktext(index: str, id: str):
    chunktext=""
    chunkhit=EsClient.get(index=index+"_chunks", id=id)
    if "_source" in chunkhit and "text" in chunkhit["_source"]:
        chunktext=chunkhit["_source"]["text"]

    return jsonify(chunktext=chunktext, id=id)        
@app.route("/pdf/<path:filename>")
def serve_pdf(filename):
    try:
        return send_file(
            f"{filename}",
            mimetype="application/pdf",
            as_attachment=False
        )
    except FileNotFoundError:
        abort(404)
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

    response, chunks = utils.rag_query(EsClient, CONFIG["index"] + "_chunks", 5, EmbeddingModel, message, document_id=docids)
    
    sources = utils.chunks_to_sources(EsClient, CONFIG["index"], chunks)
    return jsonify({"response": response, "sources": sources})

    

if __name__ == '__main__':
    utils.buildLog(CONFIG["index"])
    app.run(debug=True, host='0.0.0.0', port=5001)
