package org.gazzax.labs.solrdf;

import static org.gazzax.labs.solrdf.TestUtility.randomInt;
import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link F} class.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class FTestCase {
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
	}
	
	@Test
	public void isHybrid() { 
		final SolrQueryRequest request = mock(SolrQueryRequest.class);
		final ModifiableSolrParams [] listOfParams = {
			new ModifiableSolrParams().add(FacetParams.FACET, "true"),
			new ModifiableSolrParams().add(CommonParams.START, String.valueOf(randomInt())),
			new ModifiableSolrParams().add(CommonParams.ROWS, String.valueOf(randomInt())),
			new ModifiableSolrParams() 
				.add(FacetParams.FACET, "true")
				.add(CommonParams.START, String.valueOf(randomInt())),
			new ModifiableSolrParams()
				.add(CommonParams.START, "true")
				.add(CommonParams.ROWS, String.valueOf(randomInt())),
			new ModifiableSolrParams()
				.add(FacetParams.FACET, "true")
				.add(CommonParams.ROWS, String.valueOf(randomInt())),
				new ModifiableSolrParams()
			.add(FacetParams.FACET, "true")
			.add(CommonParams.START, String.valueOf(randomInt()))
			.add(CommonParams.ROWS, String.valueOf(randomInt())),
		};
		
		for (final ModifiableSolrParams params : listOfParams) {
			when(request.getParams()).thenReturn(params);
			assertTrue(F.isHybrid(request));
			reset(request);
		}
	}
	
	@Test
	public void isntHybrid() { 
		final SolrQueryRequest request = mock(SolrQueryRequest.class);
		final ModifiableSolrParams [] listOfParams = {
			new ModifiableSolrParams(),
			new ModifiableSolrParams().add(FacetParams.FACET, "false"),
		};
		
		for (final ModifiableSolrParams params : listOfParams) {
			when(request.getParams()).thenReturn(params);
			assertFalse(F.isHybrid(request));
			reset(request);
		}
	}	
	
	@Test
	public void readCommandFromIncomingStream() throws Exception {
		final String command = randomString();
		final ContentStream stream = new ContentStreamBase.ByteArrayStream(command.getBytes(), randomString());
		
		assertEquals(command.trim(), F.readCommandFromIncomingStream(stream).trim());
	}
	
	@Test
	public void readCommandFromIncomingEmptyStream() throws Exception {
		final String command = "";
		final ContentStream stream = new ContentStreamBase.ByteArrayStream(command.getBytes(), randomString());
		
		assertEquals(command.trim(), F.readCommandFromIncomingStream(stream).trim());
	}	
}