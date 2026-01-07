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
GET heb/_search?filter_path=*.*.*.huridocs
{
  "query": {
    "bool": {
      "must": [
        {
          "exists": {
            "field": "huridocs"
          }
        }
      ]
    }
  }

}
get pdfs/_doc/86eed63a3401439c54b727a686a841e
