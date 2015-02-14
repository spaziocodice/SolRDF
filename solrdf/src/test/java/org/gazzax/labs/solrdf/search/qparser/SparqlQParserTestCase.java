package org.gazzax.labs.solrdf.search.qparser;

import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.mockito.Mockito.mock;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link SparqlQParser} test case.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlQParserTestCase {
	private QParser cut;
	
	private SolrQueryRequest request;
	private SolrParams localParams;
	private SolrParams params;
	
	@Before
	public void setUp() {
		request = mock(SolrQueryRequest.class);
	}
	
	/**
	 * If a null query string is given, then an exception must be raised.
	 */	
	@Test
	public void nullQueryString() {
		cut = new SparqlQParser(null, localParams, params, request);
		
		try {
			cut.parse();
		} catch(final SyntaxError expected) {
			// Nothing, as this is the expected behaviour
		}
	}
	
	/**
	 * If an invalid query string is given, then an exception must be raised.
	 */
	@Test
	public void invalidQueryString() {
		cut = new SparqlQParser(randomString(), localParams, params, request);
		
		try {
			cut.parse();
		} catch(final SyntaxError expected) {
			// Nothing, as this is the expected behaviour
		}		
	}
	
	/**
	 * Positive test case.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Test
	public void validQueryString() throws Exception {
		cut = new SparqlQParser("SELECT * WHERE { ?s ?p ?o }", localParams, params, request);
		
		try {
			cut.parse();
		} catch(final SyntaxError expected) {
			// Nothing, as this is the expected behaviour
		}		
	}	
}