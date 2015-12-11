package org.gazzax.labs.solrdf.handler.search.ldf;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.handler.search.Sparql11SearchHandler;

public class AvailableDatasetsSearchHandler extends RequestHandlerBase {
	final static String SELECT_ALL_DATASETS = 
			"CONSTRUCT { ?s ?p ?o } "
			+ "WHERE { "
			+ "			?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/ns/void#Dataset> . "
			+ "			?s ?p ?o "
			+ "}";
	
	@Override
	public void handleRequestBody(final SolrQueryRequest request, final SolrQueryResponse response) throws Exception {
		request.setParams(
				new ModifiableSolrParams(request.getParams())
					.set(Names.QUERY, SELECT_ALL_DATASETS));
		requestHandler(request).handleRequest(request, response);
	}

	@Override
	public String getDescription() {
		return "Returns the available datasets within this SolRDF instance";
	}
	
	@Override
	public String getSource() {
		return "https://github.com/agazzarini/SolRDF";
	}
	
	/**
	 * Returns the {@link SolrRequestHandler} associated with the given name.
	 * 
	 * @param request the current Solr request.
	 * @param name the {@link SolrRequestHandler} name.
	 * @return the {@link SolrRequestHandler} associated with the given name.
	 */ 
	SolrRequestHandler requestHandler(final SolrQueryRequest request) {
		return request.getCore().getRequestHandler(Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);
	}	
}