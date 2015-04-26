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
import java.util.Iterator;
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

	private final String downloadsQuery = "p:<http\\://example.org/ns#downloads>";
	private final String priceQuery = "p:<http\\://example.org/ns#price>";
	private final String dateQuery = "p:<http\\://purl.org/dc/elements/1.1/date>";

	private final String _0 = "0";
	private final String _10 = "10";	
	private final String _100 = "100";
	private final String _1000 = "1000";
	private final String _1_15 = "1.15";
	private final String _30_50 = "30.50";
	
	private final String aStartDate = "2000-01-01T00:00:00Z";
	private final String anEndDate = "2020-12-30T00:00:00Z";
	private final String gapExpression ="+1YEAR";
	
	private Map<String, Integer> expectedDownloadsOccurrencesWithAtLeastOneOccurrence = new HashMap<String, Integer>();
	{
		expectedDownloadsOccurrencesWithAtLeastOneOccurrence.put("0", 1);
		expectedDownloadsOccurrencesWithAtLeastOneOccurrence.put("100", 3);
		expectedDownloadsOccurrencesWithAtLeastOneOccurrence.put("400", 1);
	}	

	private Map<String, Integer> expectedDownloadsOccurrencesWithNoMincount = new HashMap<String, Integer>();
	{
		expectedDownloadsOccurrencesWithNoMincount.put("0", 1);
		expectedDownloadsOccurrencesWithNoMincount.put("100", 3);
		expectedDownloadsOccurrencesWithNoMincount.put("200", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("300", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("400", 1);
		expectedDownloadsOccurrencesWithNoMincount.put("500", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("600", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("700", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("800", 0);
		expectedDownloadsOccurrencesWithNoMincount.put("900", 0);
	}	
	
	private Map<String, Integer> expectedReviewedOccurrences = new HashMap<String, Integer>();
	{
		expectedReviewedOccurrences.put("true", 3);
		expectedReviewedOccurrences.put("false", 1);
	}	
	
	private Map<String, Integer> expectedPricesOccurrences = new HashMap<String, Integer>();
	{
		expectedPricesOccurrences.put("1.15", 1);
		expectedPricesOccurrences.put("11.15", 1);
		expectedPricesOccurrences.put("21.15", 4);
	}		
	
	private Map<String, Integer> expectedDatesOccurrences = new HashMap<String, Integer>();
	{
		expectedDatesOccurrences.put("2000-01-01T00:00:00Z", 1);
		expectedDatesOccurrences.put("2001-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2002-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2003-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2004-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2005-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2006-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2007-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2008-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2009-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2010-01-01T00:00:00Z", 3);
		expectedDatesOccurrences.put("2011-01-01T00:00:00Z", 1);
		expectedDatesOccurrences.put("2012-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2013-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2014-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2015-01-01T00:00:00Z", 1);
		expectedDatesOccurrences.put("2016-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2017-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2018-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2019-01-01T00:00:00Z", 0);
		expectedDatesOccurrences.put("2020-01-01T00:00:00Z", 0);		
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
	public void numHintAsDefaultWithNonAliasedQuery() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				randomString(), 
				null, 
				expectedDownloadsOccurrencesWithNoMincount);
	}
	
	@Test
	public void numHintAsDefaultWithAliasedQuery() throws Exception {	
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				randomString(), 
				randomString(), 
				expectedDownloadsOccurrencesWithNoMincount);		
	}

	@Test
	public void numHintAsDefaultWithAliasedQuery2() throws Exception {	
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				null, 
				randomString(), 
				expectedDownloadsOccurrencesWithNoMincount);		
	}

	/**
	 * In case a given hint is unknown, then "num" will be used.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void numHintAsDefaultWithNonAliasedQuery2() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				null, 
				null, 
				expectedDownloadsOccurrencesWithNoMincount);
	}	
	
	/**
	 * Facet mincount is default equal to 0.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void mincountIsZeroByDefault() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				randomString(), 
				null, 
				expectedDownloadsOccurrencesWithNoMincount);
	}
	
	/**
	 * Facet mincount can be explicitly set.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void explicitZeroMinCount() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT, 0);
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				randomString(), 
				expectedDownloadsOccurrencesWithNoMincount);
	}
	
	@Test
	public void explicitOneMinCountNotAliased() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT, 1);
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				null, 
				expectedDownloadsOccurrencesWithAtLeastOneOccurrence);
	}
	
	@Test
	public void explicitOneMinCountWithAliasing() throws Exception {	
		query.set(FacetParams.FACET_MINCOUNT, 1);
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				randomString(), 
				expectedDownloadsOccurrencesWithAtLeastOneOccurrence);		
	}
	
	/**
	 * A single string facet query without alias and a mincount equals to 2.
	 * The facet is keyed with its query.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void atLeastTwoOccurrences() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT, 2);
		
		expectedDownloadsOccurrencesWithAtLeastOneOccurrence.remove("0");
		expectedDownloadsOccurrencesWithAtLeastOneOccurrence.remove("400");
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				null, 
				expectedDownloadsOccurrencesWithAtLeastOneOccurrence);
	}
	
	/**
	 * A single string facet range query without alias.
	 * The facet is keyed with its query.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneNumericFacetWithoutAlias() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				null, 
				expectedDownloadsOccurrencesWithNoMincount);
	}
	
	/**
	 * A single string facet object query with alias.
	 * The facet is keyed with the provided alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneNumericFacetWithAlias() throws Exception {
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				randomString(), 
				expectedDownloadsOccurrencesWithNoMincount);
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
				_0,
				_1000,
				_100,
				"num", 
				null, 
				expectedDownloadsOccurrencesWithNoMincount);
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
				_0,
				_1000,
				_100,
				"num", 
				randomString(), 
				expectedDownloadsOccurrencesWithNoMincount);
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
				_1_15,
				_30_50,
				_10,
				FacetQuery.NUMERIC_HINT, 
				null, 
				expectedPricesOccurrences,
				"31.15");
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
				_1_15,
				_30_50,
				_10,
				FacetQuery.NUMERIC_HINT, 
				randomString(), 
				expectedPricesOccurrences,
				"31.15");
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
				aStartDate,
				anEndDate,
				gapExpression,
				FacetQuery.DATE_HINT, 
				null, 
				expectedDatesOccurrences,
				"2021-01-01T00:00:00Z");
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
				aStartDate,
				anEndDate,
				gapExpression,
				FacetQuery.DATE_HINT, 
				randomString(), 
				expectedDatesOccurrences,
				"2021-01-01T00:00:00Z");
	}

	/**
	 * A single date facet object query with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDateFacetWithMinOccurrences() throws Exception {
		for(final Iterator<Entry<String,Integer>> iterator = expectedDatesOccurrences.entrySet().iterator(); iterator.hasNext();) {
			final Entry<String, Integer> entry = iterator.next();
			if (0 == entry.getValue()) {
				iterator.remove();
			}
		}
		
		query.set(FacetParams.FACET_MINCOUNT, 1);		
		assertOneFacetRangeQuery(
				dateQuery, 
				aStartDate,
				anEndDate,
				gapExpression,
				FacetQuery.DATE_HINT, 
				randomString(), 
				expectedDatesOccurrences,
				"2021-01-01T00:00:00Z");
	}

	/**
	 * Five facets without aliases.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void severalRangeFacetsWithoutAlias() throws Exception {
		assertFacetQueries(
				asList(
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						(String)null,
						(String)null,
						(String)null),
				asList(_0, _1_15, aStartDate),		
				asList(_1000,_30_50, anEndDate),
				asList(_100, _10, gapExpression),
				asList(
						expectedDownloadsOccurrencesWithNoMincount, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}		
	
	/**
	 * Five facets aliased.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void severalRangeFacetsWithAlias() throws Exception {
		assertFacetQueries(
				asList(
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						randomString(),
						randomString()),
				asList(_0, _1_15, aStartDate),		
				asList(_1000,_30_50, anEndDate),
				asList(_100, _10, gapExpression),						
				asList(
						expectedDownloadsOccurrencesWithNoMincount, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}			
	
	/**
	 * Five facets aliased and not aliased.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void severalFacetsWithAndWithoutAlias() throws Exception {
		assertFacetQueries(
				asList(
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						null,
						randomString()),
				asList(_0, _1_15, aStartDate),		
				asList(_1000,_30_50, anEndDate),
				asList(_100, _10, gapExpression),						
				asList(
						expectedDownloadsOccurrencesWithNoMincount, 
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
		query.set(FacetParams.FACET_MINCOUNT + ".1", "1");
		assertFacetQueries(
				asList(
						downloadsQuery, 
						priceQuery, 
						dateQuery), 
				asList(
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.NUMERIC_HINT, 
						FacetQuery.DATE_HINT), 
				asList(
						randomString(),
						null,
						randomString()),
				asList(_0, _1_15, aStartDate),		
				asList(_1000,_30_50, anEndDate),
				asList(_100, _10, gapExpression),						
				asList(
						expectedDownloadsOccurrencesWithAtLeastOneOccurrence, 
						expectedPricesOccurrences, 
						expectedDatesOccurrences));
	}			
	
	/**
	 * Executes the current {@link SolrQuery} and returns back the {@link NamedList} containing the collected facet object queries.
	 * 
	 * @return the collected facet object queries.
	 * @throws SolrServerException in case of Solr request processing failure.
	 */
	private NamedList<?> executeQueryAndGetFacetObjectRangeQueries() throws SolrServerException {
		final QueryResponse queryResponse = solr.query(query);
		final NamedList<Object> response = queryResponse.getResponse();
		assertNotNull(response);

		final NamedList<?> facetCounts = (NamedList<?>) response.get("facet_counts");
		assertNotNull(facetCounts);
		final NamedList<?> facetObjectQueries = (NamedList<?>) facetCounts.get("facet_object_ranges_queries");
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
	 * @param facetQuery the facet (range) query.
	 * @param hint the facet query hint.
	 * @param alias the facet query alias.
	 * @param expectedResults the expected results.
	 * 
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	private void assertOneFacetRangeQuery(
			final String facetQuery, 
			final String start, 
			final String end, 
			final String gap,
			final String hint, 
			final String alias,
			final Map<String, Integer> expectedResults,
			final String ... expectedEnd) throws Exception {
		
		if (alias != null) {
			query.set("facet.range.q.alias", alias);		
		}

		query.set("facet.range.start", start);		
		query.set("facet.range.end", end);		
		query.set("facet.range.gap", gap);		
		query.set("facet.range.q.hint", hint);		
		query.set("facet.range.q", facetQuery);
		
		final NamedList<?> facets = executeQueryAndGetFacetObjectRangeQueries();
		assertEquals(1, facets.size());
		
		NamedList<?> facet = null;
		if (alias != null) {
			assertNull(facets.get(facetQuery));
			facet = (NamedList<?>) facets.get(alias);	
			assertFacetResults(expectedResults, (NamedList<?>)facet.get("counts"));
		} else {
			assertNull(facets.get(alias));
			facet = (NamedList<?>) facets.get(facetQuery);
			assertFacetResults(expectedResults, (NamedList<?>)facet.get("counts"));
		}
		
		String endExpectaction = expectedEnd != null && expectedEnd.length > 0 ? expectedEnd[0] : end;
		
		assertEquals(start, facet.get("start"));
		assertEquals(endExpectaction, facet.get("end"));
		assertEquals(gap, facet.get("gap"));
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
			final List<String> starts,
			final List<String> ends,
			final List<String> gaps,
			final List<Map<String, Integer>> expectedResults) throws Exception {
	
		for (int i = 0; i < facetQueries.size(); i++) {
			final String queryId = "." + (i + 1);
			final String alias = aliases.get(i);
			final String hint = hints.get(i);
			final String facetQuery = facetQueries.get(i);
			final String start = starts.get(i);
			final String end = ends.get(i);
			final String gap = gaps.get(i);
			
			if (alias != null) {
				query.set("facet.range.q.alias" + queryId, alias);		
			}
			
			if (start != null) {
				query.set("facet.range.start" + queryId, start);						
			}

			if (end != null) {
				query.set("facet.range.end" + queryId, end);						
			}

			if (gap != null) {
				query.set("facet.range.gap" + queryId, gap);						
			}

			query.set("facet.range.q.hint" + queryId, hint);		
			query.set("facet.range.q" + queryId, facetQuery);
		}
		
		final NamedList<?> facetObjectRangeQueries = executeQueryAndGetFacetObjectRangeQueries();
		assertEquals(facetQueries.size(), facetObjectRangeQueries.size());
		
		NamedList<?> facet = null;
		for (int i = 0; i < facetQueries.size(); i++) {
			final String alias = aliases.get(i);
			final String facetQuery = facetQueries.get(i);
			final Map<String, Integer> expectation = expectedResults.get(i);
			if (alias != null) {
				assertNull(facetObjectRangeQueries.get(facetQuery));
				facet = (NamedList<?>) facetObjectRangeQueries.get(alias);	
				assertFacetResults(expectation, (NamedList<?>) facet.get("counts"));
			} else {
				assertNull(facetObjectRangeQueries.get(alias));
				facet = (NamedList<?>) facetObjectRangeQueries.get(facetQuery);	
				assertFacetResults(expectation, (NamedList<?>) facet.get("counts"));
			}
		}
	}	

	@Override
	protected String examplesDirectory() {
		throw new IllegalStateException();
	}	
}