DELETE pdfs_chunks

GET pdfs_chunks/_search

GET pdfs/_doc/a85931c1446b9e7b27d435348e13cc


GET pdfs/_search?filter_path=*.*.*.grobid.abstract.text,*.*.*.huridocs.title,*.total.value
{
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "Training strategies for athletes in  basketball",
            "fields": [
              "*"
            ]
          }
        }
      ]
    }
  }

}
GET pdfs/_search?filter_path=*.*.*.meta.title,*.*.*.grobid.metadata.title,grobid.title,*.*.*.huridocs.title,*.*.*.grobid.abstract,*.*.highlight
{
  "highlight": {
    "fields": {
      "grobid.abstract.text": {
      "highlight_query": {
        "match": {"grobid.abstract.text": 
           "azulay several magnetic"
        }
      }
    },
      "meta.title": {
      "highlight_query": {
        "match": {"meta.title": 
           "azulay several magnetic"
         
        }
      } 
    }
  }},
  "size": 100,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "azulay",
            "fields": [
              "*"
            ]
          }
        }
      ]
    }
  }

}
GET pdfs/_search?filter_path=*.*.*
{
  "query": {
    "term": {
      "file.extension": "dwg"
    }
  }

}
get pdfs/_search
{ "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "query": "equations to estimate length and threedimensional"
                        
                    }
                }
            ]
        }
    }}
get pdfs/_search
{
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "file.extension"
          }
        }
      ]
    }
  }
}

POST /pdfs_chunks/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": " 
      if (ctx._source.containsKey('text') && ctx._source.text != null) { 
        ctx._source.text_length = ctx._source.text.length(); 
      } else { 
        ctx._source.text_length = 0; 
      }
    "
  }
}