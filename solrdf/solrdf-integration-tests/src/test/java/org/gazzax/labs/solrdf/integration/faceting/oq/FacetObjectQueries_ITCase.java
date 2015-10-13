package org.gazzax.labs.solrdf.integration.faceting.oq;

import static java.util.Arrays.asList;
import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.gazzax.labs.solrdf.handler.search.faceting.FacetQuery;
import org.gazzax.labs.solrdf.handler.search.faceting.RDFacetComponent;
import org.gazzax.labs.solrdf.handler.search.faceting.oq.FacetObjectQuery;
import org.gazzax.labs.solrdf.integration.faceting.FacetTestSupertypeLayer;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Facet Object Queries integration test.
 * 
 * Note: since we don't have a corresponding Solrj for SolRDF, the result section would get 
 * an error because we don't have plain SolrDocument here. That's the reason we will use Solrj 
 * but we will always requests zero rows-
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */  
@Ignore
public class FacetObjectQueries_ITCase extends FacetTestSupertypeLayer {	
	private final String publisherQuery = "p:<http\\://purl.org/dc/elements/1.1/publisher>";
	private final String reviewedQuery = "p:<http\\://example.org/ns#reviewed>";
	
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
	 * In case the given hint is unknown, then "str" will be used.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void strHintAsDefault() throws Exception {
		assertOneFacetQuery(
				publisherQuery, 
				randomString(), 
				null, 
				expectedPublishersOccurrences);
		
		assertOneFacetQuery(
				publisherQuery, 
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		assertOneFacetQuery(
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
		final NamedList<?> facetObjectQueries = (NamedList<?>) facetCounts.get(RDFacetComponent.OBJECT_QUERIES);
		assertNotNull(facetObjectQueries);
		return facetObjectQueries;
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
	private void assertOneFacetQuery(
			final String facetQuery, 
			final String hint, 
			final String alias,
			final Map<String, Integer> expectedResults) throws Exception {
		
		if (alias != null) {
			query.set(FacetObjectQuery.QUERY_ALIAS, alias);		
		}
		
		query.set(FacetObjectQuery.QUERY_HINT, hint);		
		query.set(FacetObjectQuery.QUERY, facetQuery);
		
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
				query.set(FacetObjectQuery.QUERY_ALIAS + queryId, alias);		
			}
			
			query.set(FacetObjectQuery.QUERY_HINT + queryId, hint);		
			query.set(FacetObjectQuery.QUERY + queryId, facetQuery);
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
}