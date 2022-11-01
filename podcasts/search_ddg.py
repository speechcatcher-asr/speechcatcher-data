import json
from duckduckgo_search import ddg

keywords = '"Hosted on Acast. See acast.com/privacy for more information." site:feeds.acast.com'
results = ddg(keywords, region='de-de', safesearch='Off', time=None, max_results=300)

with open('ddg_search.json', 'w') as outfile:
    outfile.write(json.dumps(results, indent=4))    
