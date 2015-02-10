package org.gazzax.labs.solrdf.handler.search.handler;

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

		// Although a stupid Map could (apparently) avoid the conditional logic, here we have just 4 
		// possible entries (GET, POST, PUT and DELETE), so a set of if statements is almost innocue.
		if ("GET".equals(method)) {
			final String graphUri = parameters.get("default") != null ? null : parameters.get("graph");
			final String q = new StringBuilder("CONSTRUCT { ?s ?p ?o } WHERE ")
				.append(graphUri != null ? "{ GRAPH <" + graphUri + "> { ?s ?p ?o } } " : "{ ?s ?p ?o }" )
				.toString();
			
			request.setParams(new ModifiableSolrParams(parameters).add(CommonParams.Q, q));
			request.getCore()
				.getRequestHandler(
						parameters.get(
								SEARCH_HANDLER_PARAMETER_NAME,
								DEFAULT_SEARCH_HANDLER_NAME))
				.handleRequest(request, response);
		} else if ("POST".equals(method)) {
			if (request.getContentStreams() == null || !request.getContentStreams().iterator().hasNext()) {
				throw new SolrException(ErrorCode.BAD_REQUEST, "Emtpty RDF Payload");
			}
			
			// FIXME: RDF payload must be associated with the given graph
//			final String graphUri = parameters.get("default") != null ? null : parameters.get("graph");
			
			request.getCore()
				.getRequestHandler(
					parameters.get(
							BULK_UPDATE_HANDLER_PARAMETER_NAME,
							DEFAULT_BULK_UPDATE_HANDLER_NAME))
					.handleRequest(request, response);
		} else if ("PUT".equals(method)) {
			// We never fall within this case (see class comments)
			if (request.getContentStreams() == null || !request.getContentStreams().iterator().hasNext()) {
				throw new SolrException(ErrorCode.BAD_REQUEST, "Emtpty RDF Payload");
			}
			final String graphUri = parameters.get("default") != null ? null : parameters.get("graph");
			final String q = new StringBuilder("DROP SILENT ")
				.append(graphUri != null ? "GRAPH <" + graphUri + "> " : "DEFAULT" )
				.toString();
			
			request.setParams(new ModifiableSolrParams(parameters).add(CommonParams.Q, q));
			request.getCore()
				.getRequestHandler(
					parameters.get(
							UPDATE_HANDLER_PARAMETER_NAME,
							DEFAULT_UPDATE_HANDLER_NAME))
							.handleRequest(request, response);
			
			request.getCore()
				.getRequestHandler(
					parameters.get(
							BULK_UPDATE_HANDLER_PARAMETER_NAME,
							DEFAULT_BULK_UPDATE_HANDLER_NAME))
					.handleRequest(request, response);
		
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
}
