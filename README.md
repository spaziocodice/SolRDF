<table><tr style="border:none"><td style="border:none"><img src="https://cloud.githubusercontent.com/assets/7569632/7524584/5971e1ba-f503-11e4-940e-72e808677c48.png" width="100" height="100"/></td><td style="border:none"><h1>Apache Solr meets RDF</h1></td></tr></table>

SolRDF (i.e. Solr + RDF) is a set of Solr extensions for managing (index and search) RDF data. Join us at solrdf-user-list@googlegroups.com

[![Continuous Integration status](https://travis-ci.org/agazzarini/SolRDF.svg?branch=master)](https://travis-ci.org/agazzarini/SolRDF)

This page will guide you through the SolRDF quick installation. 

> If you already have Solr (4.8.x - 4.10.x) installed, please refer to [this section](https://github.com/agazzarini/SolRDF/wiki/User%20Guide#if-you-already-have-solr-installed) for detailed instruction.    

I assume you already have Java (7), Maven (3.x) and git on your system.

Checkout the project Open a new shell and type the following:

> cd /tmp  
> git clone https://github.com/agazzarini/SolRDF.git solrdf-download

Build and run SolrRDF

> cd solrdf-download/solrdf  
> mvn clean package cargo:run  

The very first time you run this command a lot of things will be downloaded, Solr included. At the end you should see sheomething like this:

[INFO] Jetty 7.6.15.v20140411 Embedded started on port [8080]
[INFO] Press Ctrl-C to stop the container... 

SolRDF is up and running! Now let's add some data. Open a new shell and type the following

> curl -v http://localhost:8080/solr/store/update/bulk?commit=true \  
  -H "Content-Type: application/n-triples" \  
  --data-binary @/tmp/solrdf-download/solrdf/src/test/resources/sample_data/bsbm-generated-dataset.nt  

Ok, you just added about 5000 triples. Now, it's time to execute some query:

> curl "http://127.0.0.1:8080/solr/store/sparql" \  
  --data-urlencode "q=SELECT * WHERE { ?s ?p ?o } LIMIT 10" \  
  -H "Accept: application/sparql-results+xml"  

> ...      

>  curl "http://127.0.0.1:8080/solr/store/sparql" \   
  --data-urlencode "q=SELECT * WHERE { ?s ?p ?o } LIMIT 10" \  
  -H "Accept: application/sparql-results+json"  
  
>  ...

Et voilà! Enjoy! As you can imagine I'm still working on that...so if you meet some annoying bug feel free to give me a shout ;)
