package org.gazzax.labs.solrdf.integration.faceting.orq;

import static java.util.Arrays.asList;
import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
import org.gazzax.labs.solrdf.handler.search.faceting.RDFacetComponent;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.FacetRangeQuery;
import org.gazzax.labs.solrdf.integration.faceting.FacetTestSupertypeLayer;
import org.junit.Test;

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
public class FacetObjectRangeQueries_ITCase extends FacetTestSupertypeLayer {
	private final String _0 = "0";
	private final String _10 = "10";	
	private final String _100 = "100";
	private final String _1000 = "1000";
	private final String _1_15 = "1.15";
	private final String _30_50 = "30.50";
	
	private final String aStartDate = "2000-01-01T00:00:00Z";
	private final String anEndDate = "2020-12-30T00:00:00Z";
	private final String gapExpression ="+1YEAR";
	
	private Map<String, Integer> expectedDownloadsRangesWithAtLeastOneOccurrence = new HashMap<String, Integer>();
	{
		expectedDownloadsRangesWithAtLeastOneOccurrence.put("0", 1);
		expectedDownloadsRangesWithAtLeastOneOccurrence.put("100", 3);
		expectedDownloadsRangesWithAtLeastOneOccurrence.put("400", 1);
	}	

	private Map<String, Integer> expectedDownloadsRangesWithNoMincount = new HashMap<String, Integer>();
	{
		expectedDownloadsRangesWithNoMincount.put("0", 1);
		expectedDownloadsRangesWithNoMincount.put("100", 3);
		expectedDownloadsRangesWithNoMincount.put("200", 0);
		expectedDownloadsRangesWithNoMincount.put("300", 0);
		expectedDownloadsRangesWithNoMincount.put("400", 1);
		expectedDownloadsRangesWithNoMincount.put("500", 0);
		expectedDownloadsRangesWithNoMincount.put("600", 0);
		expectedDownloadsRangesWithNoMincount.put("700", 0);
		expectedDownloadsRangesWithNoMincount.put("800", 0);
		expectedDownloadsRangesWithNoMincount.put("900", 0);
	}	
	
	private Map<String, Integer> expectedPriceRanges = new HashMap<String, Integer>();
	{
		expectedPriceRanges.put("1.15", 1);
		expectedPriceRanges.put("11.15", 1);
		expectedPriceRanges.put("21.15", 4);
	}		
	
	private Map<String, Integer> expectedDateRanges = new HashMap<String, Integer>();
	{
		expectedDateRanges.put("2000-01-01T00:00:00Z", 1);
		expectedDateRanges.put("2001-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2002-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2003-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2004-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2005-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2006-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2007-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2008-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2009-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2010-01-01T00:00:00Z", 3);
		expectedDateRanges.put("2011-01-01T00:00:00Z", 1);
		expectedDateRanges.put("2012-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2013-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2014-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2015-01-01T00:00:00Z", 1);
		expectedDateRanges.put("2016-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2017-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2018-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2019-01-01T00:00:00Z", 0);
		expectedDateRanges.put("2020-01-01T00:00:00Z", 0);		
	}			
	
	/**
	 * In case a given hint is unknown on an anonymous query, then "num" will be used.
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
				expectedDownloadsRangesWithNoMincount);
	}
	
	/**
	 * In case a given hint is unknown on an aliases query, then "num" will be used.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void numHintAsDefaultWithAliasedQuery() throws Exception {	
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				randomString(), 
				randomString(), 
				expectedDownloadsRangesWithNoMincount);		
	}

	/**
	 * In case the hint is null on an aliases query, then "num" will be used.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void numHintAsDefaultWithAliasedQuery2() throws Exception {	
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				null, 
				randomString(), 
				expectedDownloadsRangesWithNoMincount);		
	}

	/**
	 * In case the hint is null on an anonymous query, then "num" will be used.
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
				expectedDownloadsRangesWithNoMincount);
	}	
	
	/**
	 * Facet mincount is 0 by default.
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
				expectedDownloadsRangesWithNoMincount);
	}
	
	/**
	 * Facet mincount can be explicitly set to 0.
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
				expectedDownloadsRangesWithNoMincount);
	}
	
	/**
	 * Facet mincount can be explicitly set to 1 (anonymous query).
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */	
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
				expectedDownloadsRangesWithAtLeastOneOccurrence);
	}
	
	/**
	 * Facet mincount can be explicitly set to 1 (aliased query).
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */	
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
				expectedDownloadsRangesWithAtLeastOneOccurrence);		
	}
	
	/**
	 * A single numeric range facet without alias and a mincount equals to 2.
	 * The facet is keyed with its query.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void atLeastTwoOccurrences() throws Exception {
		query.set(FacetParams.FACET_MINCOUNT, 2);
		
		expectedDownloadsRangesWithAtLeastOneOccurrence.remove("0");
		expectedDownloadsRangesWithAtLeastOneOccurrence.remove("400");
		assertOneFacetRangeQuery(
				downloadsQuery, 
				_0,
				_1000,
				_100,
				"num", 
				null, 
				expectedDownloadsRangesWithAtLeastOneOccurrence);
	}
	
	/**
	 * A single numeric range facet query without alias.
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
				expectedDownloadsRangesWithNoMincount);
	}
	
	/**
	 * A single numeric range facet with alias.
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
				expectedDownloadsRangesWithNoMincount);
	}		
	
	/**
	 * A single numeric (integer) range facet without alias.
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
				expectedDownloadsRangesWithNoMincount);
	}
	
	/**
	 * A single numeric (integer) range facet with alias.
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
				expectedDownloadsRangesWithNoMincount);
	}	
	
	/**
	 * A single numeric (decimal) range facet without alias.
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
				expectedPriceRanges,
				"31.15");
	}
	
	/**
	 * A single numeric (integer) range facet with alias.
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
				expectedPriceRanges,
				"31.15");
	}		
	
	/**
	 * A single date range facet without alias.
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
				expectedDateRanges,
				"2021-01-01T00:00:00Z");
	}
	
	/**
	 * A single date range facet with alias.
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
				expectedDateRanges,
				"2021-01-01T00:00:00Z");
	}

	/**
	 * A single date range facet with alias.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void oneDateFacetWithMinOccurrences() throws Exception {
		for(final Iterator<Entry<String,Integer>> iterator = expectedDateRanges.entrySet().iterator(); iterator.hasNext();) {
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
				expectedDateRanges,
				"2021-01-01T00:00:00Z");
	}

	/**
	 * Several range facets (no aliases).
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
						expectedDownloadsRangesWithNoMincount, 
						expectedPriceRanges, 
						expectedDateRanges));
	}		
	
	/**
	 * Several facets (with aliasing).
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
						expectedDownloadsRangesWithNoMincount, 
						expectedPriceRanges, 
						expectedDateRanges));
	}			
	
	/**
	 * Several facets (aliased and not aliased).
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
						expectedDownloadsRangesWithNoMincount, 
						expectedPriceRanges, 
						expectedDateRanges));
	}		
	
	/**
	 * Several facets with a scoped parameter for one of them.
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
						expectedDownloadsRangesWithAtLeastOneOccurrence, 
						expectedPriceRanges, 
						expectedDateRanges));
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
		final NamedList<?> facetObjectQueries = (NamedList<?>) facetCounts.get(RDFacetComponent.RANGE_QUERIES);
		assertNotNull(facetObjectQueries);
		return facetObjectQueries;
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

		query.set(FacetParams.FACET_RANGE_START, start);		
		query.set(FacetParams.FACET_RANGE_END, end);		
		query.set(FacetParams.FACET_RANGE_GAP, gap);		
		query.set(FacetRangeQuery.QUERY_HINT, hint);		
		query.set(FacetRangeQuery.QUERY, facetQuery);
		
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
				query.set(FacetRangeQuery.QUERY_ALIAS + queryId, alias);		
			}
			
			if (start != null) {
				query.set(FacetParams.FACET_RANGE_START + queryId, start);						
			}

			if (end != null) {
				query.set(FacetParams.FACET_RANGE_END + queryId, end);						
			}

			if (gap != null) {
				query.set(FacetParams.FACET_RANGE_GAP + queryId, gap);						
			}

			query.set(FacetRangeQuery.QUERY_HINT + queryId, hint);		
			query.set(FacetRangeQuery.QUERY + queryId, facetQuery);
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
}