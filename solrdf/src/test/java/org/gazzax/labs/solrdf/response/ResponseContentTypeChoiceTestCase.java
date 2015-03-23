package org.gazzax.labs.solrdf.response;

import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpHeaders;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

/**
 * {@link HybridResponseWriter} test case.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class ResponseContentTypeChoiceTestCase {
	private HybridResponseWriter cut;
	private SolrQueryRequest request;
	private SolrQueryResponse response;
	private HttpServletRequest httpRequest;
	private Map<Object, Object> requestContext;

	private final Query constructQuery = QueryFactory
			.create("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
	private final Query selectQuery = QueryFactory
			.create("SELECT * WHERE { ?s ?p ?o }");
	private final Query askQuery = QueryFactory
			.create("PREFIX foaf: <http://xmlns.com/foaf/0.1/> ASK  { ?x foaf:name  \"Alice\" }");
	private final Query describeQuery = QueryFactory
			.create("DESCRIBE <http://example.org/>");

	/**
	 * Set up fixture for this test case.
	 */
	@Before
	public void setUp() {
		cut = new HybridResponseWriter();
		request = mock(SolrQueryRequest.class);
		response = mock(SolrQueryResponse.class);
		httpRequest = mock(HttpServletRequest.class);
		requestContext = new HashMap<Object, Object>();
		requestContext.put(Names.HTTP_REQUEST_KEY, httpRequest);
		when(request.getContext()).thenReturn(requestContext);

		final NamedList<Object> configuration = new NamedList<Object>();
		final NamedList<Object> contentTypeConfiguration = new NamedList<Object>();
		contentTypeConfiguration
				.add(String.valueOf(Query.QueryTypeSelect),
						"application/sparql-results+xml,application/sparql-results+json,text/csv,text/plain,text/tab-separated-values");
		contentTypeConfiguration.add(String.valueOf(Query.QueryTypeConstruct),
				"application/rdf+xml,application/n-triples,text/turtle");
		contentTypeConfiguration.add(String.valueOf(Query.QueryTypeDescribe),
				"application/rdf+xml,application/n-triples,text/turtle");
		contentTypeConfiguration
				.add(String.valueOf(Query.QueryTypeAsk),
						"text/csv,text/plain,text/tab-separated-values,application/sparql-results+xml,application/sparql-results+json");
		configuration.add("content-types", contentTypeConfiguration);
		cut.init(configuration);
	}

	@Test
	public void getContentType_SelectQueryWithIncomingMediaType() {
		assertIncomingRequestWithMediaType(
				selectQuery, 
				new String[] {
					ResultSetLang.SPARQLResultSetCSV.getHeaderString(),
					ResultSetLang.SPARQLResultSetJSON.getHeaderString(),
					ResultSetLang.SPARQLResultSetText.getHeaderString(),
					ResultSetLang.SPARQLResultSetTSV.getHeaderString(),
					ResultSetLang.SPARQLResultSetXML.getHeaderString() });
	}

	@Test
	public void getContentType_AskQueryWithIncomingMediaType() {
		assertIncomingRequestWithMediaType(
				askQuery, 
				new String[] {
					WebContent.contentTypeTextCSV, 
					WebContent.contentTypeTextPlain,
					WebContent.contentTypeTextTSV,
					ResultSetLang.SPARQLResultSetJSON.getHeaderString(),
					ResultSetLang.SPARQLResultSetXML.getHeaderString() });
	}

	@Test
	public void getContentType_DescribeQueryWithIncomingMediaType() {
		assertIncomingRequestWithMediaType(
				describeQuery, 
				new String[] {
					WebContent.contentTypeRDFXML, 
					WebContent.contentTypeNTriples,
					WebContent.contentTypeTurtle });
	}

	@Test
	public void getContentType_ConstructQueryWithIncomingMediaType() {
		assertIncomingRequestWithMediaType(
				constructQuery, 
				new String[] {
					WebContent.contentTypeRDFXML, 
					WebContent.contentTypeNTriples,
					WebContent.contentTypeTurtle });
	}

	@Test
	public void getContentType_SelectQueryWithoutIncomingMediaType() {
		assertIncomingRequestWithoutMediaType(selectQuery);
	}

	@Test
	public void getContentType_AskQueryWithoutIncomingMediaType() {
		assertIncomingRequestWithoutMediaType(askQuery);
	}

	@Test
	public void getContentType_DescribeQueryWithoutIncomingMediaType() {
		assertIncomingRequestWithoutMediaType(describeQuery);
	}

	@Test
	public void getContentType_ConstructQueryWithoutIncomingMediaType() {
		assertIncomingRequestWithoutMediaType(constructQuery);
	}

	@Test
	public void getContentType_SelectQueryWithInvalidMediaType() {
		assertIncomingRequestWithInvalidMediaType(selectQuery);
	}

	@Test
	public void getContentType_AskQueryWithInvalidMediaType() {
		assertIncomingRequestWithInvalidMediaType(askQuery);
	}

	@Test
	public void getContentType_DescribeQueryWithInvalidMediaType() {
		assertIncomingRequestWithInvalidMediaType(describeQuery);
	}

	@Test
	public void getContentType_ConstructQueryWithInvalidMediaType() {
		assertIncomingRequestWithInvalidMediaType(constructQuery);
	}

	/**
	 * Internal method used for asserting the behaviour of the system in case
	 * the incoming request contains a valid media type.
	 * 
	 * @param query the query, which can be one of Ask, Select, Construct, Describe.
	 * @param mediaTypes the media types that are supposed to be supported.
	 */
	private void assertIncomingRequestWithMediaType(final Query query, final String[] mediaTypes) {
		requestContext.put(Names.QUERY, query);

		for (final String mediaType : mediaTypes) {
			assertSupportedMediaType(mediaType);
			assertSupportedMediaType(mediaType + ";q=0.2;charset=utf8;level=2");
		}
	}

	/**
	 * Compose-method that internally asserts a supported media type.
	 * 
	 * @param mediaType the media type.
	 */
	private void assertSupportedMediaType(final String mediaType) {
		when(httpRequest.getHeader(HttpHeaders.ACCEPT)).thenReturn(mediaType);
		String contentType = cut.getContentType(request, false);
		assertNotNull(contentType);
		assertEquals(cut.cleanMediaType(mediaType), contentType);

		contentType = cut.getContentType(request, response);
		assertNotNull(contentType);
		assertEquals(cut.cleanMediaType(mediaType), contentType);
	}

	/**
	 * Internal method used for asserting the behaviour of the system in case
	 * the incoming request doesn't contain a valid media type.
	 * 
	 * @param query the query, which can be one of Ask, Select, Construct, Describe.
	 * @param mediaTypes the media types that are supposed to be supported.
	 */
	private void assertIncomingRequestWithoutMediaType(final Query query) {
		requestContext.put(Names.QUERY, query);
		when(httpRequest.getHeader(HttpHeaders.ACCEPT)).thenReturn(null);
		String contentType = cut.getContentType(request, false);
		assertNotNull(contentType);
		assertEquals(
				cut.contentTypeChoiceStrategies.get(query.getQueryType()).getContentType(null), 
				contentType);

		contentType = cut.getContentType(request, false);
		assertNotNull(contentType);
		assertEquals(
				cut.contentTypeChoiceStrategies.get(query.getQueryType()).getContentType(null), 
				contentType);
	}

	/**
	 * Internal method used for asserting the behaviour of the system in case
	 * the incoming request contains an invalid media type.
	 * 
	 * @param query the query, which can be one of Ask, Select, Construct, Describe.
	 * @param mediaTypes the media types that are supposed to be supported.
	 */
	private void assertIncomingRequestWithInvalidMediaType(final Query query) {
		requestContext.put(Names.QUERY, query);

		final String invalidMediaType = randomString();
		when(httpRequest.getHeader(HttpHeaders.ACCEPT)).thenReturn(invalidMediaType);

		String contentType = cut.getContentType(request, false);
		assertNotNull(contentType);
		assertEquals(
				cut.contentTypeChoiceStrategies.get(query.getQueryType()).getContentType(
						new String[] { invalidMediaType }), 
				contentType);

		contentType = cut.getContentType(request, response);
		assertNotNull(contentType);
		assertEquals(
				cut.contentTypeChoiceStrategies.get(query.getQueryType()).getContentType(
						new String[] { invalidMediaType }), 
				contentType);
	}
}