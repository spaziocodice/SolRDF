package org.gazzax.labs.solrdf.handler.update;

import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.QueryParseException;

/**
 * Test case for {@link Sparql11UpdateRdfDataLoader}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11UpdateRdfDataLoaderTestCase {
	private Sparql11UpdateRdfDataLoader cut;
	private SolrQueryRequest request;
	private SolrQueryResponse response;
	private ContentStream stream;
	private UpdateRequestProcessor processor;
	private ModifiableSolrParams parameters;
	
	@Before
	public void setUp() {
		cut = new Sparql11UpdateRdfDataLoader();
		request = mock(SolrQueryRequest.class);
		response = mock(SolrQueryResponse.class);
		parameters = new ModifiableSolrParams();
		stream = mock(ContentStream.class);
		processor = mock(UpdateRequestProcessor.class);
		
		when(request.getParams()).thenReturn(parameters);
	}
	
	/** 
	 * In case of a null query a {@link QueryParseException} must be raised.
	 */
	@Test
	public void nullOrEmptyQuery() {
		final String [] invalidQueries = {"", "   "};
		for (final String invalidQuery : invalidQueries) {
			parameters.set(CommonParams.Q, invalidQuery);
			try {
				cut.load(request, response, stream, processor);
				fail();
			} catch (final Exception expected) {
				assertTrue(expected instanceof SolrException);
				assertEquals(ErrorCode.BAD_REQUEST.code, ((SolrException)expected).code());
			}
		}
	}
	
	/** 
	 * In case of an invalid query a {@link QueryParseException} must be raised.
	 */
	@Test
	public void invalidQuery() {
		final String [] invalidQueries = {"BLABALBALABLA", randomString()};
		
		for (final String invalidQuery : invalidQueries) {
			parameters.set(CommonParams.Q, invalidQuery);
			try {
				cut.load(request, response, stream, processor);
				fail();
			} catch (final Exception expected) {
				assertTrue(expected instanceof QueryParseException);
			}					
		}
	}
}