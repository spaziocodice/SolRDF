package org.gazzax.labs.solrdf.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.TestUtility;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link Sparql11GraphStoreProtocolHandler}.
 * 
 * Quite difficult to implement this test as the {@link SolrCore} class is final and 
 * therefore Mockito is not able to mock it! Damn! So at the end it is not actually complete.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11GraphStoreProtocolHandlerTestCase {
	
	private Sparql11GraphStoreProtocolHandler cut;
	private HttpServletRequest request;
	private SolrQueryRequest solrQueryRequest;
	private SolrQueryResponse solrQueryResponse;
	private ModifiableSolrParams requestParams;
	private Set<String> requestedRequestHandlers;
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		requestedRequestHandlers = new HashSet<String>();
		cut = new Sparql11GraphStoreProtocolHandler() {
			void forward(final SolrQueryRequest request, final SolrQueryResponse response, final String requestHandlerName, final String defaultRequestHandlerName) {
				requestedRequestHandlers.add(requestHandlerName);
				requestedRequestHandlers.add(defaultRequestHandlerName);
			};
		};
		
		request = mock(HttpServletRequest.class);
		solrQueryRequest = mock(SolrQueryRequest.class);
		solrQueryResponse = mock(SolrQueryResponse.class);
		requestParams = new ModifiableSolrParams();
						
		when(solrQueryRequest.getParams()).thenReturn(requestParams);
		Map<Object, Object> requestContext = new HashMap<Object, Object>();
		requestContext.put(Names.HTTP_REQUEST_KEY, request);
		
		when(solrQueryRequest.getContext()).thenReturn(requestContext);
	}
	
	/**
	 * A request that uses the HTTP GET method MUST retrieve an RDF payload that is a serialization of the named graph paired with the graph IRI in the Graph Store. 
	 * If the request doesn't contain a named graph (by means of graphUri parameter) or it contains a "default" parameter or it doesn't contain any parameter
	 * then it is supposed to be associated with the default graph.
	 */
	@Test
	public void getDefaultGraphWithoutParameters() {
		when(request.getMethod()).thenReturn("GET");
		
		assertNull(requestParams.get(Names.GRAPH_URI_PARAMETER_NAME));
				
		cut.handleRequest(solrQueryRequest, solrQueryResponse);
		
		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.SEARCH_HANDLER_PARAMETER_NAME);
		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.DEFAULT_SEARCH_HANDLER_NAME);
		
		assertTrue(requestedRequestHandlers.toString(), requestedRequestHandlers.isEmpty());
	}

	/**
	 * A request that uses the HTTP GET method MUST retrieve an RDF payload that is a serialization of the named graph paired with the graph IRI in the Graph Store. 
	 * If the request doesn't contain a named graph (by means of graphUri parameter) or it contains a "default" parameter or it doesn't contain any parameter
	 * then it is supposed to be associated with the default graph.
	 */
	@Test
	public void getDefaultGraphWithExplicitDefaultGraphParameter() {
		when(request.getMethod()).thenReturn("GET");
		
		assertNull(requestParams.get(Names.GRAPH_URI_PARAMETER_NAME));
		
		requestParams.add(Names.DEFAULT_GRAPH_PARAMETER_NAME, "");
		cut.handleRequest(solrQueryRequest, solrQueryResponse);

		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.SEARCH_HANDLER_PARAMETER_NAME);
		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.DEFAULT_SEARCH_HANDLER_NAME);
		assertTrue(requestedRequestHandlers.toString(), requestedRequestHandlers.isEmpty());
	}

	/**
	 * A request that uses the HTTP GET method MUST retrieve an RDF payload that is a serialization of the named graph paired with the graph IRI in the Graph Store. 
	 * If the request doesn't contain a named graph (by means of graphUri parameter) or it contains a "default" parameter or it doesn't contain any parameter
	 * then it is supposed to be associated with the default graph.
	 */
	@Test
	public void getNamedGraph() {
		when(request.getMethod()).thenReturn("GET");
		requestParams.add(Names.GRAPH_URI_PARAMETER_NAME, TestUtility.DUMMY_BASE_URI);
		
		cut.handleRequest(solrQueryRequest, solrQueryResponse);
		
		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.SEARCH_HANDLER_PARAMETER_NAME);
		requestedRequestHandlers.remove(Sparql11GraphStoreProtocolHandler.DEFAULT_SEARCH_HANDLER_NAME);
		assertTrue(requestedRequestHandlers.toString(), requestedRequestHandlers.isEmpty());
	}	
}