package org.gazzax.labs.solrdf.handler;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a SPARQL 1.1 Graph Store protocol endpoint.
 * 
 * Note that the protocol is not completed supported: PUT and DELETE are reserved methods in {@link SolrRequestParsers}: they are supposed to be 
 * used only with /schema and /config requests.
 * 
 * So at the moment we have only support for GET and POST requests.
 * 
 * @see http://www.w3.org/TR/sparql11-http-rdf-update
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11GraphStoreProtocolHandler extends RequestHandlerBase {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(Sparql11GraphStoreProtocolHandler.class));
	
	static final String SEARCH_HANDLER_PARAMETER_NAME = "s";
	static final String DEFAULT_SEARCH_HANDLER_NAME = "/sparql-query";
	
	static final String UPDATE_HANDLER_PARAMETER_NAME = "u";
	static final String DEFAULT_UPDATE_HANDLER_NAME = "/sparql-update";
	
	static final String BULK_UPDATE_HANDLER_PARAMETER_NAME = "b";
	static final String DEFAULT_BULK_UPDATE_HANDLER_NAME = "/update/bulk";
	
	@Override
	public void handleRequestBody(
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws Exception {
		
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY);
		final String method = httpRequest.getMethod();
 
		final SolrParams parameters = request.getParams();
		final String graphUri = parameters.get(Names.DEFAULT_GRAPH_PARAMETER_NAME) != null 
				? null 
				: parameters.get(Names.GRAPH_URI_PARAMETER_NAME);
		
		LOGGER.debug(MessageCatalog._00093_GSP_REQUEST, method, graphUri != null ? graphUri : "default");
		
		// Although a stupid Map could (apparently) avoid the conditional logic, here we have just 4 
		// possible entries (GET, POST, PUT and DELETE), so a set of if statements is almost innocue.
		if ("GET".equals(method)) {
			request.setParams(
					new ModifiableSolrParams(parameters)
						.add(CommonParams.Q, constructQuery(graphUri)));
			
			forward(request, response, SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME);
		} else if ("POST".equals(method)) {
			if (request.getContentStreams() == null || !request.getContentStreams().iterator().hasNext()) {
				throw new SolrException(ErrorCode.BAD_REQUEST, "Empty RDF Payload");
			}
			
			if (graphUri != null) {
				request.setParams(
						new ModifiableSolrParams(parameters).add(
								Names.GRAPH_URI_ATTRIBUTE_NAME, 
								graphUri));
			}			
			forward(request, response, BULK_UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_BULK_UPDATE_HANDLER_NAME);
		} else if ("PUT".equals(method)) {
			// Unfortunately we never fall within this case (see class comments)
			if (request.getContentStreams() == null || !request.getContentStreams().iterator().hasNext()) {
				throw new SolrException(ErrorCode.BAD_REQUEST, "Emtpty RDF Payload");
			}
			
			final String q = new StringBuilder("DROP SILENT ")
				.append(graphUri != null ? "GRAPH <" + graphUri + "> " : "DEFAULT" )
				.toString();
			
			request.setParams(new ModifiableSolrParams(parameters).add(CommonParams.Q, q));

			forward(request, response, UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME);
			forward(request, response, BULK_UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_BULK_UPDATE_HANDLER_NAME);
		} else if ("DELETE".equals(method)) {
			// Unfortunately we never fall within this case (see class comments)
		}
	}

	@Override
	public String getDescription() {
		return "(Pseudo) Implementation of a SPARQL 1.1 Graph Store protocol endpoint.";
	}

	@Override
	public String getSource() {
		return "https://github.com/agazzarini/SolRDF";
	}
	
	/**
	 * Returns a CONSTRUCT query used in GET requests.
	 * 
	 * @param graphUri the graphUri (optional).
	 * @return a CONSTRUCT query used in GET requests.
	 */
	String constructQuery(final String graphUri) {
		return new StringBuilder("CONSTRUCT { ?s ?p ?o } WHERE ")
			.append(graphUri != null 
				? new StringBuilder("{ GRAPH <").append(graphUri).append("> { ?s ?p ?o } } ")
				: "{ ?s ?p ?o }" )
			.toString();
	}
	
	/**
	 * Forwards the control to the appropriate {@link RequestHandlerBase}.
	 * 
	 * @param request the current Solr query request.
	 * @param response the current Solr query response.
	 * @param requestHandlerName the target requestHandler name.
	 * @param defaultRequestHandlerName the default request handler name (in case of absence of the preceding handler).
	 */
	void forward(final SolrQueryRequest request, final SolrQueryResponse response, final String requestHandlerName, final String defaultRequestHandlerName) {
		request.getCore()
			.getRequestHandler(
				request.getParams().get(requestHandlerName, defaultRequestHandlerName))
			.handleRequest(request, response);		
	}
}
