package org.gazzax.labs.solrdf.integration.orq;

import static java.util.Arrays.asList;
import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;
import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.gazzax.labs.solrdf.handler.search.faceting.FacetQuery;
import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Facet Object Range Queries integration test.
 * 
 * Note: since we don't have a corresponding Solrj for SolRDF, the result section would get 
 * an error because we don't have plain SolrDocument here. That's the reason we will use Solrj 
 * but we will always requests zero rows-
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */  
public class FacetObjectRangeQueries_ITCase extends IntegrationTestSupertypeLayer {
	private final static String TEST_DATA_URI = new File("src/test/resources/sample_data/faceting_test_dataset.nt").toURI().toString();
	
	private SolrQuery query;

	private final String reviewedQuery = "p:<http\\://example.org/ns#reviewed>";
	private final String downloadsQuery = "p:<http\\://example.org/ns#downloads>";
	private final String priceQuery = "p:<http\\://example.org/ns#price>";
	private final String dateQuery = "p:<http\\://purl.org/dc/elements/1.1/date>";
	
	private Map<String, Integer> expectedPublishersOccurrences = new HashMap<String, Integer>();
	{
		expectedPublishersOccurrences.put("ABCD Publishing", 2);
		expectedPublishersOccurrences.put("Acme Publishing", 2);
		expectedPublishersOccurrences.put("Packt Publishing", 1);
		expectedPublishersOccurrences.put("CDEF Publishing", 1);
	}
	
	private Map<String, Integer> expectedPublishersWithAtLeastTwoOccurrences = new HashMap<String, Integer>();
	{
		expectedPublishersWithAtLeastTwoOccurrences.put("ABCD Publishing", 2);
		expectedPublishersWithAtLeastTwoOccurrences.put("Acme Publishing", 2);
	}
	
	private Map<String, Integer> expectedReviewedOccurrences = new HashMap<String, Integer>();
	{
		expectedReviewedOccurrences.put("true", 3);
		expectedReviewedOccurrences.put("false", 1);
	}	
	
	private Map<String, Integer> expectedDownloadsOccurrences = new HashMap<String, Integer>();
	{
		expectedDownloadsOccurrences.put("192", 2);
		expectedDownloadsOccurrences.put("442", 1);
		expectedDownloadsOccurrences.put("99", 1);
		expectedDownloadsOccurrences.put("199", 1);
	}		
	
	private Map<String, Integer> expectedPricesOccurrences = new HashMap<String, Integer>();
	{
		expectedPricesOccurrences.put("22.1", 1);
		expectedPricesOccurrences.put("23.95", 1);
		expectedPricesOccurrences.put("22.4", 1);
		expectedPricesOccurrences.put("20", 1);
		expectedPricesOccurrences.put("11", 1);
		expectedPricesOccurrences.put("22", 1);
	}		
	
	private Map<String, Integer> expectedDatesOccurrences = new HashMap<String, Integer>();
	{
		expectedDatesOccurrences.put("2000-12-30T23:00:00Z", 1);
		expectedDatesOccurrences.put("2010-06-22T22:00:00Z", 1);
		expectedDatesOccurrences.put("2010-10-09T22:00:00Z", 1);
		expectedDatesOccurrences.put("2010-12-31T23:00:00Z", 1);
		expectedDatesOccurrences.put("2011-09-06T22:00:00Z", 1);
		expectedDatesOccurrences.put("2015-08-22T22:00:00Z", 1);
	}			
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * @throws IOException 
	 * @throws SolrServerException 
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@BeforeClass
	public final static void loadSampleData() throws SolrServerException, IOException {
		final Model memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(TEST_DATA_URI, DUMMY_BASE_URI, "N-TRIPLES");
  
		DATASET.add(memoryModel);
		
		commitChanges();
		
		final Model model = DATASET.getModel();
		  
		assertFalse(model.isEmpty());
		assertTrue(model.isIsomorphicWith(memoryModel));
	}
	
	/**
	 * Setup fixture for this test.
	 * 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	@Before
	public void init() throws Exception {
		query = new SolrQuery("SELECT * WHERE { ?s ?p ?o }");
		query.setRows(0);
		query.setFacet(true);
		query.setRequestHandler("/sparql");
	}
	
	/**
	 * In case a given hint is unknown, then "num" will be used.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void numHintAsDefault() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				randomString(), 
				null, 
				expectedPublishersOccurrences);
		
		assertOneFacetRangeQuery(
				downloadsQuery, 
				randomString(), 
				randomString(), 
				expectedPublishersOccurrences);		
	}
	
	/**
	 * Facet mincount must be greater than 0. If not so, then 1 will be used as default value.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void mincountMustBeAtLeastOne() throws Exception {
		query.set("facet.mincount", -1);	
		oneStringFacetWithAlias();

		query.set("facet.mincount", 0);	
		oneStringFacetWithAlias();
	}
	
	/**
	 * A single string facet object query without alias and a mincount equals to 2.
	 * The facet is keyed with its query.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void atLeastTwoOccurrences() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT, 2);
		assertOneFacetRangeQuery(
				publisherQuery, 
				FacetQuery.STRING_HINT, 
				null, 
				expectedPublishersWithAtLeastTwoOccurrences);
	}
	
	/**
	 * A single string facet object query without alias.
	 * The facet is keyed with its query.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneStringFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				publisherQuery, 
				FacetQuery.STRING_HINT, 
				null, 
				expectedPublishersOccurrences);
	}
	
	/**
	 * A single string facet object query with alias.
	 * The facet is keyed with the provided alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneStringFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				publisherQuery, 
				FacetQuery.STRING_HINT, 
				randomString(), 
				expectedPublishersOccurrences);
	}		
	
	/**
	 * A single boolean facet object query without alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneBooleanFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				reviewedQuery, 
				FacetQuery.BOOLEAN_HINT, 
				null, 
				expectedReviewedOccurrences);
	}
	
	/**
	 * A single boolean facet object query with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneBooleanFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				reviewedQuery, 
				FacetQuery.BOOLEAN_HINT, 
				randomString(), 
				expectedReviewedOccurrences);
	}	
	
	/**
	 * A single numeric (integer) facet object query without alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneIntegerFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				FacetQuery.NUMERIC_HINT, 
				null, 
				expectedDownloadsOccurrences);
	}
	
	/**
	 * A single numeric (integer) facet object query with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneIntegerFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				FacetQuery.NUMERIC_HINT, 
				randomString(), 
				expectedDownloadsOccurrences);
	}	
	
	/**
	 * A single numeric (decimal) facet object query without alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDecimalFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				priceQuery, 
				FacetQuery.NUMERIC_HINT, 
				null, 
				expectedPricesOccurrences);
	}
	
	/**
	 * A single numeric (integer) facet object query with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDecimalFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				priceQuery, 
				FacetQuery.NUMERIC_HINT, 
				randomString(), 
				expectedPricesOccurrences);
	}		
	
	/**
	 * A single date facet object query without alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDateFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				dateQuery, 
				FacetQuery.DATE_HINT, 
				null, 
				expectedDatesOccurrences);
	}
	
	/**
	 * A single date facet object query with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDateFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				dateQuery, 
				FacetQuery.DATE_HINT, 
				randomString(), 
				expectedDatesOccurrences);
	}			
	
	/**
	 * Five facets without aliases.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void fiveFacetsWithoutAlias() throws Exception {
		assertFacetQueries(
				asList(
						publisherQuery, 
						reviewedQuery, 
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.STRING_HINT, 
						FacetQuery.BOOLEAN_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						(String)null,
						(String)null,
						(String)null,
						(String)null,
						(String)null),
				asList(
						expectedPublishersOccurrences, 
						expectedReviewedOccurrences, 
						expectedDownloadsOccurrences, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}		
	
	/**
	 * Five facets aliased.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void fiveFacetsWithAlias() throws Exception {
		assertFacetQueries(
				asList(
						publisherQuery, 
						reviewedQuery, 
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.STRING_HINT, 
						FacetQuery.BOOLEAN_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						randomString(),
						randomString(),
						randomString(),
						randomString()),
				asList(
						expectedPublishersOccurrences, 
						expectedReviewedOccurrences, 
						expectedDownloadsOccurrences, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}			
	
	/**
	 * Five facets aliased and not aliased.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void fiveFacetsWithAndWithoutAlias() throws Exception {
		assertFacetQueries(
				asList(
						publisherQuery, 
						reviewedQuery, 
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.STRING_HINT, 
						FacetQuery.BOOLEAN_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						null,
						null,
						randomString(),
						null),
				asList(
						expectedPublishersOccurrences, 
						expectedReviewedOccurrences, 
						expectedDownloadsOccurrences, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}		
	
	/**
	 * Five facets with a scoped parameter for one of them.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void scopedParameters() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT + ".1", "2");
		assertFacetQueries(
				asList(
						publisherQuery, 
						reviewedQuery, 
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.STRING_HINT, 
						FacetQuery.BOOLEAN_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						null,
						null,
						randomString(),
						null),
				asList(
						expectedPublishersWithAtLeastTwoOccurrences, 
						expectedReviewedOccurrences, 
						expectedDownloadsOccurrences, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}			
	
	/**
	 * Executes the current {@link SolrQuery} and returns back the {@link NamedList} containing the collected facet object queries.
	 * 
	 * @return the collected facet object queries.
	 * @throws SolrServerException in case of Solr request processing failure.
	 */
	private NamedList<?> executeQueryAndGetFacetObjectQueries() throws SolrServerException {
		final QueryResponse queryResponse = solr.query(query);
		final NamedList<Object> response = queryResponse.getResponse();
		assertNotNull(response);

		final NamedList<?> facetCounts = (NamedList<?>) response.get("facet_counts");
		assertNotNull(facetCounts);
		final NamedList<?> facetObjectQueries = (NamedList<?>) facetCounts.get("facet_object_queries");
		assertNotNull(facetObjectQueries);
		return facetObjectQueries;
	}	
	
	/**
	 * Asserts a given NamedList against an expected map of results.
	 * 
	 * @param expected the map containing expected (facet) results.
	 * @param actual the actual {@link NamedList} returned from Solr.
	 */
	private void assertFacetResults(final Map<String, Integer> expected, final NamedList<?> actual) {
		assertNotNull(actual);
		assertEquals(expected.size(), actual.size());
		
		for (final Entry<String, Integer> expectedCount : expected.entrySet()) {
			assertEquals(expectedCount.getValue(), actual.remove(expectedCount.getKey()));
		}
		assertEquals(0, actual.size());
	}	
	
	/**
	 * A compose method for avoid duplication in "oneFacet" test methods.
	 * 
	 * @param facetQuery the facet query.
	 * @param hint the facet query hint.
	 * @param alias the facet query alias.
	 * @param expectedResults the expected results.
	 * 
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	private void assertOneFacetRangeQuery(
			final String facetQuery, 
			final String hint, 
			final String alias,
			final Map<String, Integer> expectedResults) throws Exception {
		
		if (alias != null) {
			query.set("facet.obj.q.alias", alias);		
		}
		
		query.set("facet.obj.q.hint", hint);		
		query.set("facet.obj.q", facetQuery);
		
		final NamedList<?> facetObjectQueries = executeQueryAndGetFacetObjectQueries();
		assertEquals(1, facetObjectQueries.size());
		
		if (alias != null) {
			assertNull(facetObjectQueries.get(facetQuery));
			assertFacetResults(expectedResults, (NamedList<?>) facetObjectQueries.get(alias));
		} else {
			assertNull(facetObjectQueries.get(alias));
			assertFacetResults(expectedResults, (NamedList<?>) facetObjectQueries.get(facetQuery));
		}
	}
	
	
	
	/**
	 * A compose method for avoid duplication in "nFacet" test methods.
	 * 
	 * @param facetQuery the facet query.
	 * @param hint the facet query hint.
	 * @param alias the facet query alias.
	 * @param expectedResults the expected results.
	 * 
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	private void assertFacetQueries(
			final List<String> facetQueries, 
			final List<String> hints, 
			final List<String> aliases,
			final List<Map<String, Integer>> expectedResults) throws Exception {
	
		for (int i = 0; i < facetQueries.size(); i++) {
			final String queryId = "." + (i + 1);
			final String alias = aliases.get(i);
			final String hint = hints.get(i);
			final String facetQuery = facetQueries.get(i);
			
			if (alias != null) {
				query.set("facet.obj.q.alias" + queryId, alias);		
			}
			
			query.set("facet.obj.q.hint" + queryId, hint);		
			query.set("facet.obj.q" + queryId, facetQuery);
		}
		
		final NamedList<?> facetObjectQueries = executeQueryAndGetFacetObjectQueries();
		assertEquals(facetQueries.size(), facetObjectQueries.size());
		
		for (int i = 0; i < facetQueries.size(); i++) {
			final String alias = aliases.get(i);
			final String facetQuery = facetQueries.get(i);
			final Map<String, Integer> expectation = expectedResults.get(i);
			if (alias != null) {
				assertNull(facetObjectQueries.get(facetQuery));
				assertFacetResults(expectation, (NamedList<?>) facetObjectQueries.get(alias));
			} else {
				assertNull(facetObjectQueries.get(alias));
				assertFacetResults(expectation, (NamedList<?>) facetObjectQueries.get(facetQuery));
			}
		}
	}	

	@Override
	protected String examplesDirectory() {
		throw new IllegalStateException();
	}	
}