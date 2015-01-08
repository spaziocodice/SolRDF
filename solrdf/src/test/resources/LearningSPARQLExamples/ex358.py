# filename: ex358.py
# Send SPARQL query to SPARQL endpoint, store and output result.

import urllib2

endpointURL = "http://dbpedia.org/sparql"
query = """
SELECT ?elvisbday WHERE {
  <http://dbpedia.org/resource/Elvis_Presley>
  <http://dbpedia.org/property/dateOfBirth> ?elvisbday .
}
"""
escapedQuery = urllib2.quote(query)
requestURL = endpointURL + "?query=" + escapedQuery
request = urllib2.Request(requestURL)

result = urllib2.urlopen(request)
print result.read()

