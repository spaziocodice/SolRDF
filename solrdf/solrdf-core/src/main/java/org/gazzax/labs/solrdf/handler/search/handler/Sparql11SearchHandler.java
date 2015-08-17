package org.gazzax.labs.solrdf.handler.search.handler;
import static org.gazzax.labs.solrdf.F.readCommandFromIncomingStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.gazzax.labs.solrdf.Strings.*;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.riot.WebContent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Names;

/**
 * A RequestHandler that dispatches SPARQL 1.1 Query and Update requests across dedicated handlers.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11SearchHandler extends RequestHandlerBase {
	static final String MISSING_QUERY_IN_GET_MESSAGE = "SPARQL Protocol violation: query or q parameter is mandatory in GET requests. "
			+ "(see http://www.w3.org/TR/sparql11-protocol/#query-via-get)";
	
	static final String MISSING_QUERY_OR_UPDATE_IN_POST_MESSAGE = "SPARQL Protocol violation: POST with URL encoded parameters request without \"query\", \"q\" or \"update\" parameter. "
			+ "(see http://www.w3.org/TR/sparql11-protocol/#query-via-post-urlencoded and http://www.w3.org/TR/sparql11-protocol/#update-via-post-urlencoded)";
	
	static final String MISSING_QUERY_IN_POST_BODY = "SPARQL Protocol violation: Query request via POST directly seems to have an empty body";
	static final String MISSING_UPDATE_IN_POST_BODY = "SPARQL Protocol violation: Update request via POST directly seems to have an empty body.";
	static final String BAD_POST_REQUEST = 
			"SPARQL Protocol violation: Cannot determine the POST request type you sent. Content-type must be " 
			+ WebContent.contentTypeSPARQLQuery
			+ " or "
			+ WebContent.contentTypeSPARQLUpdate +
			" and body must contains a valid query or update command.";
	
	static final String INVALID_HTTP_METHOD = "SPARQL Protocol violation: request method must be GET or POST.";
	
	static final String SEARCH_HANDLER_PARAMETER_NAME = "s";
	static final String DEFAULT_SEARCH_HANDLER_NAME = "/sparql-query";
	
	static final String UPDATE_HANDLER_PARAMETER_NAME = "u";
	static final String DEFAULT_UPDATE_HANDLER_NAME = "/sparql-update";

	@Override
	public void handleRequestBody(final SolrQueryRequest request, final SolrQueryResponse response) throws Exception {
		final SolrParams parameters = request.getParams();
		if (isUsingGET(request)) {
			if (containsQueryParameter(parameters)) {
				requestHandler(
						request,
						parameters.get(SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME))
					.handleRequest(request, response);	
			} else {
				throw new SolrException(
						ErrorCode.BAD_REQUEST, 
						MISSING_QUERY_IN_GET_MESSAGE);
			}
		} else if (isUsingPOST(request)) {
			if (isUsingURLEncodedParameters(request)) {
				if (containsUpdateParameter(parameters)) {
					requestHandler(
							request,
							parameters.get(UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME))
						.handleRequest(new SparqlUpdateSolrQueryRequest(request), response);	
				} else if (containsQueryParameter(parameters)) {
					requestHandler(
							request,
							parameters.get(SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME))
						.handleRequest(request, response);	
				} else {    
					throw new SolrException(
							ErrorCode.BAD_REQUEST, 
							MISSING_QUERY_OR_UPDATE_IN_POST_MESSAGE);					
				}
			} else if (isSparqlQueryContentType(request)) {
				if (isBodyNotEmpty(request)) {
					request.setParams(new ModifiableSolrParams(parameters).set(Names.QUERY, readCommandFromIncomingStream(request.getContentStreams().iterator().next())));
					requestHandler(
							request,
							parameters.get(SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME))
						.handleRequest(new SparqlQuerySolrQueryRequest(request), response);	
				} else {
					throw new SolrException(
							ErrorCode.BAD_REQUEST, 
							MISSING_QUERY_IN_POST_BODY);
				} 
			} else if (isSparqlUpdateContentType(request)){ 
				if (isBodyNotEmpty(request)) {
					requestHandler(
							request,
							parameters.get(UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME))
						.handleRequest(request, response);	
				} else {
					throw new SolrException(
							ErrorCode.BAD_REQUEST, 
							MISSING_UPDATE_IN_POST_BODY);
				}
			} else {
				throw new SolrException(
						ErrorCode.BAD_REQUEST, 
						BAD_POST_REQUEST);
			}
		} else {
			throw new SolrException(
					ErrorCode.BAD_REQUEST,
					INVALID_HTTP_METHOD);
		}
	}

	/**
	 * Checks if the current (HTTP) request contains a valid body.
	 * 
	 * @param request the Solr request.
	 * @return true if the current (HTTP) request contains a valid body, false otherwise.
	 */
	boolean isBodyNotEmpty(final SolrQueryRequest request) {
		return request.getContentStreams() != null && request.getContentStreams().iterator().hasNext();
	}
	
	/**
	 * Returns true if the method associated with the current HTTP request is GET.
	 * 
	 * @param request the current Solr request.
	 * @return true if the method associated with the current HTTP request is GET.
	 */
	boolean isUsingGET(final SolrQueryRequest request) {
		return "GET".equals(((HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY)).getMethod());
	}
	
	/**
	 * Returns true if the method associated with the current HTTP request is POST.
	 * 
	 * @param request the current Solr request.
	 * @return true if the method associated with the current HTTP request is POST.
	 */
	boolean isUsingPOST(final SolrQueryRequest request) {
		return "POST".equals(((HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY)).getMethod());
	}	

	@Override
	public String getDescription() {
		return "SPARQL 1.1 Search Handler";
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
	SolrRequestHandler requestHandler(final SolrQueryRequest request, final String name) {
		return request.getCore().getRequestHandler(name);
	}
	
	/**
	 * Returns true if the current request is using POST method and URL-encoded parameters.
	 * 
	 * @param request the current Solr request.
	 * @return true if the current request is using POST method and URL-encoded parameters.
	 */
	boolean isUsingURLEncodedParameters(final SolrQueryRequest request) {
		return contentType(request).startsWith(WebContent.contentTypeHTMLForm);
	}
	
	/**
	 * Returns true if the current request has a "application/sparql-update" content type.
	 * 
	 * @param request the current Solr request.
	 * @return true if the current request has a "application/sparql-update" content type.
	 */
	boolean isSparqlUpdateContentType(final SolrQueryRequest request) {
		return contentType(request).startsWith(WebContent.contentTypeSPARQLUpdate);
	}
	
	/**
	 * Returns true if the current request has a "application/sparql-query" content type.
	 * 
	 * @param request the current Solr request.
	 * @return true if the current request has a "application/sparql-query" content type.
	 */
	boolean isSparqlQueryContentType(final SolrQueryRequest request) {
		return contentType(request).startsWith(WebContent.contentTypeSPARQLQuery); 
	}	
	
	/**
	 * Returns true if the current request contains the "query" or "q" parameter.
	 * 
	 * @param parameters the parameters associated with the current request.
	 * @return true if the current request contains the "query" or "q" parameter.
	 */
	boolean containsQueryParameter(final SolrParams parameters) {
		return parameters.get(Names.QUERY) != null || parameters.get(CommonParams.Q) != null;
	}	
	
	/**
	 * Returns true if the current request contains the "update" parameter.
	 * 
	 * @param parameters the parameters associated with the current request.
	 * @return true if the current request contains the "update" parameter.
	 */
	boolean containsUpdateParameter(final SolrParams parameters) {
		return parameters.get(Names.UPDATE_PARAMETER_NAME) != null;
	}
	
	/**
	 * Returns the content type associated with the current request.
	 * 
	 * @param request the current request.
	 * @return the content type associated with the current request or an empty string.
	 */
	String contentType(final SolrQueryRequest request) {
		final String incomingContentType =  ((HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY)).getContentType();
		return isNotNullOrEmptyString(incomingContentType) ? incomingContentType : EMPTY_STRING;
	}
	
	/**
	 * A simple wrapper around a {@link SolrQueryRequest} for marking SPARQL Queries (with POST Directly requests).
	 *  
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	static class SparqlQuerySolrQueryRequest implements SolrQueryRequest {
		final SolrQueryRequest request;
		
		/**
		 * Builds a new {@link SparqlUpdateSolrQueryRequest} with the given wrapped request.
		 * 
		 * @param request the current Solr request.
		 */
		public SparqlQuerySolrQueryRequest(final SolrQueryRequest request) {
			this.request = request;
		}
		
		@Override
		public void updateSchemaToLatest() {
			request.updateSchemaToLatest();
		}
		
		@Override
		public void setParams(final SolrParams params) {
			request.setParams(params);
		}
		
		@Override
		public long getStartTime() {
			return request.getStartTime();
		}
		
		@Override
		public SolrIndexSearcher getSearcher() {
			return request.getSearcher();
		}
		
		@Override
		public IndexSchema getSchema() {
			return request.getSchema();
		}
		
		@Override
		public SolrParams getParams() {
			return request.getParams();
		}
		
		@Override
		public String getParamString() {
			return request.getParamString();
		}
		
		@Override
		public SolrParams getOriginalParams() {
			return request.getOriginalParams();
		}
		
		@Override
		public SolrCore getCore() {
			return request.getCore();
		}
		
		@Override
		public Map<Object, Object> getContext() {
			return request.getContext();
		}
		
		@Override
		public Iterable<ContentStream> getContentStreams() {
			return null;
		}
		
		@Override
		public void close() {
			request.close();
		}		
	}		
	
	/**
	 * A simple wrapper around a {@link SolrQueryRequest} for marking SPARQL Updates (with URL encoded parameters).
	 *  
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	static class SparqlUpdateSolrQueryRequest implements SolrQueryRequest {
		final static List<ContentStream> DUMMY_STREAMS = new ArrayList<ContentStream>(1);
		static {
			DUMMY_STREAMS.add(new ContentStreamBase.ByteArrayStream(new byte[0], "dummy") {
				@Override
				public String getContentType() {
					return WebContent.contentTypeHTMLForm;
				}
			});
		}
		final SolrQueryRequest request;
		
		/**
		 * Builds a new {@link SparqlUpdateSolrQueryRequest} with the given wrapped request.
		 * 
		 * @param request the current Solr request.
		 */
		public SparqlUpdateSolrQueryRequest(final SolrQueryRequest request) {
			this.request = request;
		}
		
		@Override
		public void updateSchemaToLatest() {
			request.updateSchemaToLatest();
		}
		
		@Override
		public void setParams(final SolrParams params) {
			request.setParams(params);
		}
		
		@Override
		public long getStartTime() {
			return request.getStartTime();
		}
		
		@Override
		public SolrIndexSearcher getSearcher() {
			return request.getSearcher();
		}
		
		@Override
		public IndexSchema getSchema() {
			return request.getSchema();
		}
		
		@Override
		public SolrParams getParams() {
			return request.getParams();
		}
		
		@Override
		public String getParamString() {
			return request.getParamString();
		}
		
		@Override
		public SolrParams getOriginalParams() {
			return request.getOriginalParams();
		}
		
		@Override
		public SolrCore getCore() {
			return request.getCore();
		}
		
		@Override
		public Map<Object, Object> getContext() {
			return request.getContext();
		}
		
		@Override
		public Iterable<ContentStream> getContentStreams() {
			return DUMMY_STREAMS;
		}
		
		@Override
		public void close() {
			request.close();
		}		
	}	
}