package org.gazzax.labs.solrdf.handler.search.handler;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class Sparql11SearchHandler extends RequestHandlerBase {
	static String SEARCH_HANDLER_PARAMETER_NAME = "s";
	static String DEFAULT_SEARCH_HANDLER_NAME = "/sparql-query";
	
	static String UPDATE_HANDLER_PARAMETER_NAME = "u";
	static String DEFAULT_UPDATE_HANDLER_NAME = "/sparql-update";
	
	@Override
	public void handleRequestBody(
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws Exception {
		final SolrParams parameters = request.getParams();
		final Iterable<ContentStream> contentStreams = request.getContentStreams();
		request.getCore().getRequestHandler(
				(contentStreams == null || !contentStreams.iterator().hasNext())
					? parameters.get(SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME) 
					: parameters.get(UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME))
			.handleRequest(request, response);
	}

	@Override
	public String getDescription() {
		return getClass().getName();
	}

	@Override
	public String getSource() {
		return "N.A.";
	}
}