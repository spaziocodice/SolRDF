package org.gazzax.labs.solrdf.search.component;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link SparqlSearchComponent} test case.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlSearchComponentTestCase {
	private SparqlSearchComponent cut;
	private SolrQueryRequest request;
	private ModifiableSolrParams params = new ModifiableSolrParams();
	
	private final static String SAMPLE_SPARQL_SELECT = "SELECT * WHERE { ?s ?p ?o }";
	private final static String ANOTHER_SAMPLE_SPARQL_SELECT = "SELECT ?s WHERE { ?s ?p ?o }";
	
	@Before
	public void setUp() {
		cut = new SparqlSearchComponent();
		request = mock(SolrQueryRequest.class);
		when(request.getParams()).thenReturn(params);
	}
	
	/**
	 * Query string can be passed in request with the "q" parameter.
	 */
	@Test
	public void queryStringIn_q_parameter() {
		params.add(CommonParams.Q, SAMPLE_SPARQL_SELECT);
		
		final String queryString = cut.queryString(request);
		assertEquals(SAMPLE_SPARQL_SELECT, queryString);
	}
	
	/**
	 * Query string can be passed in request with the "query" parameter.
	 */
	@Test
	public void queryStringIn_query_parameter() {
		params.add(CommonParams.QUERY, SAMPLE_SPARQL_SELECT);
		
		final String queryString = cut.queryString(request);
		assertEquals(SAMPLE_SPARQL_SELECT, queryString);
	}	
	
	/**
	 * If both "q" and "query" parameters have been specified, then "q" got a higher priority.
	 */
	@Test
	public void queryAndQ() {
		params.add(CommonParams.QUERY, SAMPLE_SPARQL_SELECT);
		params.add(CommonParams.Q, ANOTHER_SAMPLE_SPARQL_SELECT);
		
		final String queryString = cut.queryString(request);
		assertEquals(ANOTHER_SAMPLE_SPARQL_SELECT, queryString);
	}		
	
	/**
	 * If the incoming request doesn't contain a query then an exception is thrown.
	 */
	@Test
	public void nullQuery() {
		assertNull(request.getParams().get(CommonParams.Q));
		assertNull(request.getParams().get(CommonParams.QUERY));
		
		try {
			cut.queryString(request);
		} catch (final SolrException expected) {
			assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, expected.code());			
		}
	}			
}