SolRDF
======

SolRDF (i.e. Solr + RDF) is a set of Solr extensions for managing (index and search) RDF data.

The difference between SolRDF and the jena-nosql project (specifically the Solr binding) is that in SolRDF the extensions are on the server side, while jena-nosql is a client-side project. 

The following page will guide you through the SolRDF quick installation. I assume you already have Java (7), Maven (3.x) and git on your system.

Checkout the project Open a new shell and type the following:

> cd /tmp  
> git clone https://github.com/agazzarini/SolRDF.git solrdf-download

Build and run SolrRDF

> cd solrdf-download/solrdf  
> mvn clean install cargo:run  

The very first time you run this command a lot of things will be downloaded, Solr included. At the end you should see sheomething like this:

[INFO] Jetty 7.6.15.v20140411 Embedded started on port [8080]
[INFO] Press Ctrl-C to stop the container...

SolRDF is up and running! Now let's add some data. Open a new shell and type the following

> curl -v http://localhost:8080/solr/store/update/bulk?commit=true \  
  -H "Content-Type: application/n-triples" \  
  --data-binary @/tmp/solrdf-download/solrdf/src/sample-data/bsbm-generated-dataset.nt  

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
