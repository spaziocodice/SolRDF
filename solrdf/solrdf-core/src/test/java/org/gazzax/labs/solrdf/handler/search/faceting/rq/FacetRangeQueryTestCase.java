package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import static org.gazzax.labs.solrdf.TestUtility.randomString;
import static org.junit.Assert.*;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.gazzax.labs.solrdf.Field;
import org.junit.Test;

/**
 * Test case for {@link FacetRangeQuery}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class FacetRangeQueryTestCase {
	@Test
	public void oneAnonymousQueryWithoutAlias() {
		final String q = randomString();
		final String alias = null;
		
		final SolrParams optionals = new ModifiableSolrParams()
			.add(FacetRangeQuery.QUERY, q)
			.add(FacetRangeQuery.QUERY_ALIAS, alias);
		
		final SolrParams requireds = new RequiredSolrParams(optionals);
		
		final FacetRangeQuery query = FacetRangeQuery.newAnonymousQuery(q, alias, optionals, requireds);
		
		assertEquals(q, query.query());
		assertNull(query.alias());
		assertEquals(q, query.key());
	}
	
	@Test
	public void oneAnonymousQueryWithAlias() {
		final String q = randomString();
		final String alias = randomString();
		
		final SolrParams optionals = new ModifiableSolrParams()
			.add(FacetRangeQuery.QUERY, q)
			.add(FacetRangeQuery.QUERY_ALIAS, alias);
		
		final SolrParams requireds = new RequiredSolrParams(optionals);
		
		final FacetRangeQuery query = FacetRangeQuery.newAnonymousQuery(q, alias, optionals, requireds);
		
		assertEquals(q, query.query());
		assertEquals(alias, query.alias());
		assertEquals(alias, query.key());
	}
	
	@Test
	public void moreThanOneIndexedQueryWithAlias() {
		final String [] q = { randomString(), randomString(), randomString() };
		final String [] alias = { randomString(), randomString(), randomString() };
		
		for (int i = 0; i < q.length; i++) {
			final SolrParams optionals = new ModifiableSolrParams()
				.add(FacetRangeQuery.QUERY + "." + (i + 1), q[i])
				.add(FacetRangeQuery.QUERY_ALIAS + "." + (i + 1), alias[i]);
			final SolrParams requireds = new RequiredSolrParams(optionals);
		
			final FacetRangeQuery query = FacetRangeQuery.newQuery(q[i], i + 1, optionals, requireds);
			
			assertEquals(q[i], query.query());
			assertEquals(alias[i], query.alias());
			assertEquals(alias[i], query.key());
		}
	}	
	
	@Test
	public void indexedQueryWithIndexedParameter() {
		final String [] q = { randomString(), randomString(), randomString() };
		final int [] minCount = { 3, 4, 9 };
		
		for (int i = 0; i < q.length; i++) {
			final SolrParams optionals = new ModifiableSolrParams()
				.add(FacetRangeQuery.QUERY + "." + (i + 1), q[i])
				.add(FacetParams.FACET_MINCOUNT + "." + (i + 1), String.valueOf(minCount[i]));
			final SolrParams requireds = new RequiredSolrParams(optionals);
		
			final FacetRangeQuery query = FacetRangeQuery.newQuery(q[i], i + 1, optionals, requireds);
			
			assertEquals(q[i], query.query());
			assertEquals(minCount[i], query.requiredInt(FacetParams.FACET_MINCOUNT));
		}
	}		
	
	@Test
	public void indexedQueryWithSharedParameter() {
		final String [] q = { randomString(), randomString(), randomString() };
		final int minCount = 5;
		
		for (int i = 0; i < q.length; i++) {
			final SolrParams optionals = new ModifiableSolrParams()
				.add(FacetRangeQuery.QUERY + "." + (i + 1), q[i])
				.add(FacetParams.FACET_MINCOUNT, String.valueOf(minCount));
			final SolrParams requireds = new RequiredSolrParams(optionals);
		
			final FacetRangeQuery query = FacetRangeQuery.newQuery(q[i], i + 1, optionals, requireds);
			
			assertEquals(q[i], query.query());
			assertEquals(minCount, query.requiredInt(FacetParams.FACET_MINCOUNT));
		}
	}			
	
	/**
	 * Field name is computed from query hint.
	 */
	@Test 
	public void fieldNameFromHint() {
		final String q = randomString();
		
		final ModifiableSolrParams optionals = new ModifiableSolrParams().add(FacetRangeQuery.QUERY, q);
		final SolrParams requireds = new RequiredSolrParams(optionals);
		
		FacetRangeQuery query = FacetRangeQuery.newAnonymousQuery(q, null, optionals, requireds);
		assertEquals(Field.NUMERIC_OBJECT, query.fieldName());

		optionals.set(FacetRangeQuery.QUERY_HINT, "num");
		query = FacetRangeQuery.newAnonymousQuery(q, null, optionals, requireds);
		assertEquals(Field.NUMERIC_OBJECT, query.fieldName());
		
		optionals.set(FacetRangeQuery.QUERY_HINT, "date");
		query = FacetRangeQuery.newAnonymousQuery(q, null, optionals, requireds);
		assertEquals(Field.DATE_OBJECT, query.fieldName());		
	}		
	 
	@Test
	public void optionalBoolean() { 
		final ModifiableSolrParams optionals = new ModifiableSolrParams();
		final SolrParams requireds = new RequiredSolrParams(optionals);
		
		final FacetRangeQuery query = FacetRangeQuery.newAnonymousQuery(null, null, optionals, requireds);
		assertFalse(query.optionalBoolean(FacetParams.FACET_RANGE_HARD_END));

		optionals.set(FacetParams.FACET_RANGE_HARD_END, "false");
		assertFalse(query.optionalBoolean(FacetParams.FACET_RANGE_HARD_END));
		
		optionals.set(FacetParams.FACET_RANGE_HARD_END, "true");
		assertTrue(query.optionalBoolean(FacetParams.FACET_RANGE_HARD_END));		
	}
	
	@Test
	public void optionalStrings() { 
		final ModifiableSolrParams optionals = new ModifiableSolrParams();
		final SolrParams requireds = new RequiredSolrParams(optionals);
		
		final FacetRangeQuery query = FacetRangeQuery.newAnonymousQuery(null, null, optionals, requireds);
		assertNull(query.optionalStrings(FacetParams.FACET_RANGE_INCLUDE));

		final String [] emptyArray = new String[0];
		optionals.set(FacetParams.FACET_RANGE_INCLUDE, emptyArray);
		assertSame(emptyArray, query.optionalStrings(FacetParams.FACET_RANGE_INCLUDE));

		final String [] validValue = {"a", "b", "c"};
		optionals.set(FacetParams.FACET_RANGE_INCLUDE, validValue);
		assertSame(validValue, query.optionalStrings(FacetParams.FACET_RANGE_INCLUDE));
	}	
}