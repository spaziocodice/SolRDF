package org.gazzax.labs.solrdf.handler.search;

import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.riot.WebContent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.Strings;
import org.gazzax.labs.solrdf.handler.search.Sparql11SearchHandler;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link Sparql11SearchHandler}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11SearchHandlerTestCase {
	
	private Sparql11SearchHandler cut;
	private SolrQueryRequest request;
	private SolrQueryResponse response;
	private HttpServletRequest httpRequest;
	
	/**
	 * Setup fixture for this test case.
	 */
	@Before
	public void setUp() {
		cut = spy(new Sparql11SearchHandler() {
			@Override
			SolrRequestHandler requestHandler(final SolrQueryRequest request, final String name) {
				return mock(SolrRequestHandler.class);
			}
		});
		
		request = mock(SolrQueryRequest.class);
		response = mock(SolrQueryResponse.class);
		httpRequest = mock(HttpServletRequest.class);
		
		final Map<Object, Object> context = new HashMap<Object, Object>();
		context.put(Names.HTTP_REQUEST_KEY, httpRequest);
		when(request.getContext()).thenReturn(context);
	}
	
	@Test 
	public void queryViaGET() throws Exception {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		parameters.set(Names.QUERY, randomString());
		when(httpRequest.getMethod()).thenReturn("GET");
		when(request.getParams()).thenReturn(parameters.set(Names.QUERY, randomString()));
		cut.handleRequestBody(request, response); 
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);

		reset(cut);
		
		when(request.getParams()).thenReturn(parameters.set(CommonParams.Q, randomString()));
		cut.handleRequestBody(request, response);
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);
	} 		
	
	@Test
	public void requestContainsContentType() {
		final String contentType = randomString();
		when(httpRequest.getContentType()).thenReturn(contentType);
		
		assertEquals(contentType, cut.contentType(request));
	}
	
	@Test
	public void requestDoesntContainsContentType() {
		when(httpRequest.getContentType()).thenReturn(null);
		assertEquals(Strings.EMPTY_STRING, cut.contentType(request));
	}	
	
	@Test 
	public void queryViaGETWithoutQuery() throws Exception {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		when(httpRequest.getMethod()).thenReturn("GET");
		when(request.getParams()).thenReturn(parameters);
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_QUERY_IN_GET_MESSAGE, exception.getMessage());
		}
	} 	
	
	@Test 
	public void updateUsingPOSTWithURLEncodedParameters() throws Exception {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		parameters.set(Names.UPDATE_PARAMETER_NAME, randomString());

		when(httpRequest.getMethod()).thenReturn("POST");
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm);
		when(request.getParams()).thenReturn(parameters);
		
		cut.handleRequestBody(request, response);
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_UPDATE_HANDLER_NAME);
	} 
	
	@Test 
	public void updateUsingPOSTWithURLEncodedParametersWithoutUpdateCommand() throws Exception {
		when(httpRequest.getMethod()).thenReturn("POST");
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm);
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_QUERY_OR_UPDATE_IN_POST_MESSAGE, exception.getMessage());
		}
	} 
	
	@Test 
	public void queryUsingPOSTWithURLEncodedParameters_I() throws Exception {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		parameters.set(Names.QUERY, randomString());

		when(httpRequest.getMethod()).thenReturn("POST");
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm);
		when(request.getParams()).thenReturn(parameters);
		
		cut.handleRequestBody(request, response);
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);
	} 
	
	@Test 
	public void queryUsingPOSTWithURLEncodedParameters_II() throws Exception {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		parameters.set(Names.QUERY, randomString());

		when(httpRequest.getMethod()).thenReturn("POST");
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm + ";charset=UTF-8");
		when(request.getParams()).thenReturn(parameters);
		
		cut.handleRequestBody(request, response);
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);
	} 	
	
	@Test 
	public void queryUsingPOSTWithURLEncodedParametersWithoutUpdateCommand() throws Exception {
		when(httpRequest.getMethod()).thenReturn("POST");
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm);
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_QUERY_OR_UPDATE_IN_POST_MESSAGE, exception.getMessage());
		}
	} 	
	
	@Test 
	public void updateUsingPOSTDirectly() throws Exception {
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeSPARQLUpdate);
		when(httpRequest.getMethod()).thenReturn("POST");
		
		final List<ContentStream> dummyStream = new ArrayList<ContentStream>(1);
		dummyStream.add(new ContentStreamBase.ByteArrayStream(new byte[0], "dummy") {
			@Override
			public String getContentType() {
				return WebContent.contentTypeSPARQLUpdate;
			}
		});
		when(request.getContentStreams()).thenReturn(dummyStream);
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		cut.handleRequestBody(request, response);
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_UPDATE_HANDLER_NAME);
	} 	
	
	@Test 
	public void updateUsingPOSTDirectlyWithoutBody() throws Exception {
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeSPARQLUpdate);
		when(httpRequest.getMethod()).thenReturn("POST");
		
		when(request.getContentStreams()).thenReturn(new ArrayList<ContentStream>());
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_UPDATE_IN_POST_BODY, exception.getMessage());
		}
		
		when(request.getContentStreams()).thenReturn(null);
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_UPDATE_IN_POST_BODY, exception.getMessage());
		}		
	} 		
	
	@Test 
	public void queryUsingPOSTDirectly() throws Exception {
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeSPARQLQuery);
		when(httpRequest.getMethod()).thenReturn("POST");
		
		final List<ContentStream> dummyStream = new ArrayList<ContentStream>(1);
		dummyStream.add(new ContentStreamBase.ByteArrayStream(new byte[0], "dummy") {
			@Override
			public String getContentType() {
				return WebContent.contentTypeSPARQLUpdate;
			}
		});
		when(request.getContentStreams()).thenReturn(dummyStream);
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		cut.handleRequestBody(request, response);
		
		verify(cut).requestHandler(request, Sparql11SearchHandler.DEFAULT_SPARQL_QUERY_HANDLER_NAME);
	} 	
	
	@Test 
	public void queryUsingPOSTDirectlyWithoutBody() throws Exception {
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeSPARQLQuery);
		when(httpRequest.getMethod()).thenReturn("POST");
		
		when(request.getContentStreams()).thenReturn(new ArrayList<ContentStream>());
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_QUERY_IN_POST_BODY, exception.getMessage());
		}
		
		when(request.getContentStreams()).thenReturn(null);
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.MISSING_QUERY_IN_POST_BODY, exception.getMessage());
		}		
	} 		
	
	@Test 
	public void invalidContentTypeInPOSTRequest() throws Exception {
		when(httpRequest.getContentType()).thenReturn(randomString());
		when(httpRequest.getMethod()).thenReturn("POST");
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		final List<ContentStream> dummyStream = new ArrayList<ContentStream>(1);
		dummyStream.add(new ContentStreamBase.ByteArrayStream(new byte[0], "dummy") {
			@Override
			public String getContentType() {
				return WebContent.contentTypeSPARQLUpdate;
			}
		}); 
		when(request.getContentStreams()).thenReturn(dummyStream);
		when(request.getParams()).thenReturn(new ModifiableSolrParams());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.BAD_POST_REQUEST, exception.getMessage());
		}
		
		when(request.getContentStreams()).thenReturn(null);
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.BAD_POST_REQUEST, exception.getMessage());
		}		
	} 	
	
	@Test 
	public void invalidHttpMethod() throws Exception {
		when(httpRequest.getMethod()).thenReturn(randomString());
		
		try {
			cut.handleRequestBody(request, response);
			fail();
		} catch (final Exception expected) {
			final SolrException exception = (SolrException) expected;
			assertEquals(ErrorCode.BAD_REQUEST.code, exception.code());
			assertEquals(Sparql11SearchHandler.INVALID_HTTP_METHOD, exception.getMessage());
		}
	} 	
	
	@Test
	public void requestContainsUpdateParameter() {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		assertTrue(
				cut.containsUpdateParameter(
						parameters.set(Names.UPDATE_PARAMETER_NAME, randomString())));
		assertTrue(
				cut.containsUpdateParameter(
						parameters.set(Names.UPDATE_PARAMETER_NAME, "")));		

		assertTrue(
				cut.containsUpdateParameter(
						parameters.set(Names.UPDATE_PARAMETER_NAME, "\t\t\t\t")));		
	}
	
	@Test
	public void doesntContainsUpdateParameter() {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		assertNull(parameters.get(Names.UPDATE_PARAMETER_NAME));
		
		assertFalse(cut.containsUpdateParameter(parameters));
	}	
	
	@Test
	public void containsQueryParameter() {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		assertTrue(
				cut.containsQueryParameter(
						parameters.set(Names.QUERY, randomString())));
		assertTrue(
				cut.containsQueryParameter(
						parameters.set(Names.QUERY, "")));		

		assertTrue(
				cut.containsQueryParameter(
						parameters.set(Names.QUERY, "\t\t\t\t")));	
		assertTrue(
				cut.containsQueryParameter(
						parameters.set(CommonParams.Q, randomString())));
		assertTrue(
				cut.containsQueryParameter(
						parameters.set(CommonParams.Q, "")));		

		assertTrue(
				cut.containsQueryParameter(
						parameters.set(CommonParams.Q, "\t\t\t\t")));				
	}
	
	@Test
	public void doesntContainsQueryParameter() {
		final ModifiableSolrParams parameters = new ModifiableSolrParams();
		assertNull(parameters.get(Names.QUERY));
		assertNull(parameters.get(CommonParams.Q));
		
		assertFalse(cut.containsQueryParameter(parameters));
	}		
	 
	@Test
	public void isUsingGET() {
		when(httpRequest.getMethod()).thenReturn("GET");
		assertTrue(cut.isUsingGET(request));
	}

	@Test
	public void isNotUsingGET() {
		when(httpRequest.getMethod()).thenReturn(randomString());
		assertFalse(cut.isUsingGET(request));
	}
	
	@Test
	public void isUsingPOST() {
		when(httpRequest.getMethod()).thenReturn("POST");
		assertTrue(cut.isUsingPOST(request));
	}

	@Test
	public void isNotUsingPOST() {
		when(httpRequest.getMethod()).thenReturn(randomString());
		assertFalse(cut.isUsingPOST(request));
	}	
	
	@Test
	public void isURLEncodedParameters() {
		when(httpRequest.getContentType()).thenReturn(WebContent.contentTypeHTMLForm);
		assertTrue(cut.isUsingURLEncodedParameters(request));
	}
	
	@Test
	public void isNotUsingURLEncodedParameters() {
		when(httpRequest.getContentType()).thenReturn(randomString());
		assertFalse(cut.isUsingURLEncodedParameters(request));
	}	
}