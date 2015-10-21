<img src="https://cloud.githubusercontent.com/assets/7569632/7524584/5971e1ba-f503-11e4-940e-72e808677c48.png" width="100" height="100"/>
<img src="https://cloud.githubusercontent.com/assets/7569632/7532363/51104a30-f566-11e4-8481-229f64064905.png">

SolRDF (i.e. Solr + RDF) is a set of Solr extensions for managing (index and search) RDF data. Join us at solrdf-user-list@googlegroups.com

[![Continuous Integration status](https://travis-ci.org/agazzarini/SolRDF.svg?branch=master)](https://travis-ci.org/agazzarini/SolRDF)

# Get me up and running
This section provides instructions for running SolRDF. We divided the section in two different parts because the different architecture introduced with Solr 5. Prior to that (i.e. Solr 4.x) Solr was distributed as a JEE web application and therefore, being SolRDF a Maven project, you could use Maven for starting up a live instance without downloading Solr (Maven would do that for you, behind the scenes). 

Solr 5 is now delivered as a standalone jar and therefore the SolRDF installation is different; it requires some manual steps in order to deploy configuration files and libraries within an external Solr (which needs to be downloaded separately).    

### SolRDF 1.1 (Solr 5.x)
First, you need Java 8, Apache Maven and Apache Solr installed on your machine.
Open a new shell and type the following:     

```
# cd /tmp
# git clone https://github.com/agazzarini/SolRDF.git solrdf-download
```  
#### Build and run SolrRDF    

```
# cd solrdf-download/solrdf
# mvn clean install

```
At the end of the build, after seeing

```
[INFO] --------------------------------------------------------
[INFO] Reactor Summary:
[INFO] 
[INFO] Solr RDF plugin .................... SUCCESS [  3.151 s]
[INFO] solrdf-core ........................ SUCCESS [ 10.191 s]
[INFO] solrdf-client ...................... SUCCESS [  3.554 s]
[INFO] solrdf-integration-tests ........... SUCCESS [ 14.910 s]
[INFO] --------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] --------------------------------------------------------  
[INFO] Total time: 32.065 s
[INFO] Finished at: 2015-10-20T14:42:09+01:00
[INFO] Final Memory: 43M/360M
```

you can find the solr-home directory, with everything required for running SolRDF, under the 

```
/tmp/solr/solrdf-download/solrdf/solrdf-integration-tests/target/solrdf-integration-tests-1.1-dev/solrdf
```
We refer to this directory as $SOLR_HOME. 
At this point, open a shell under the _bin_ folder of your Solr and type:

```
> ./solr -p 8080 -s $SOLR_HOME -a "-Dsolr.data.dir=/work/data/solrdf"

Waiting to see Solr listening on port 8080 [/]  
Started Solr server on port 8080 (pid=10934). Happy searching!

```

### SolRDF 1.0 (Solr 4.x)
If you're using Solr 4.x, you can point to the solrdf-1.0 branch and use the automatic procedure described below for downloading, installing and run it. There's no need to download Solr, as Maven will do that for you.

#### Checkout the project    
Open a new shell and type the following:   

```
# cd /tmp
# git clone https://github.com/agazzarini/SolRDF.git solrdf-download
```  

#### Build and run SolrRDF    

```
# cd solrdf-download/solrdf
# mvn clean install
# cd solrdf-integration-tests
# mvn clean package cargo:run
```
The very first time you run this command a lot of things will be downloaded, Solr included.
At the end you should see something like this:
```
[INFO] Jetty 7.6.15.v20140411 Embedded started on port [8080]
[INFO] Press Ctrl-C to stop the container...
``` 
[SolRDF](http://127.0.0.1:8080/solr/#/store) is up and running! 

# Add data   
Now let's add some data. You can do that in one of the following ways: 

## Browser
Open your favourite browser and type the follwing URL (line has been split for readability):
```
http://localhost:8080/solr/store/update/bulk?commit=true
&update.contentType=application/n-triples
&stream.file=/tmp/solrdf-download/solrdf/solrdf-integration-tests/src/test/resources/sample-data/bsbm-generated-dataset.nt
```
This is an example with the bundled sample data. If you have a file somehere (i.e. remotely) you can use the _stream.url_ parameter to indicate the file URL. For example:  

```
http://localhost:8080/solr/store/update/bulk?commit=true
&update.contentType=application/rdf%2Bxml
&stream.url=http://ec.europa.eu/eurostat/ramon/rdfdata/countries.rdf
```
## Command line
Open a shell and type the following
```
# curl -v http://localhost:8080/solr/store/update/bulk?commit=true \ 
  -H "Content-Type: application/n-triples" \
  --data-binary @/tmp/solrdf-download/solrdf/src/test/resources/sample_data/bsbm-generated-dataset.nt
```
Ok, you just added (about) [5000 triples](http://127.0.0.1:8080/solr/#/store). 

# SPARQL 1.1. endpoint    
SolRDF is a fully compliant SPARQL 1.1. endpoint. In order to issue a query just run a query like this:
```
# curl "http://127.0.0.1:8080/solr/store/sparql" \
  --data-urlencode "q=SELECT * WHERE { ?s ?p ?o } LIMIT 10" \
  -H "Accept: application/sparql-results+json"
  
Or  
  
# curl "http://127.0.0.1:8080/solr/store/sparql" \
  --data-urlencode "**q=SELECT * WHERE { ?s ?p ?o } LIMIT 10**" \
  -H "Accept: application/sparql-results+xml"

```

-----------------------------------

_The SolRDF logo was kindly provided by Umberto Basili_ 
