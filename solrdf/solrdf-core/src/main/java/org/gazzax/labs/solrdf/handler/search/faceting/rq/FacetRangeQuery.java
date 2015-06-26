package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.handler.search.faceting.FacetQuery;

/**
 * A value object for encapsulating a facet range query with all related parameters.
 * 
 * Apart from some parameter like alias, hint, all other parameters are described in the Solr Wiki or
 * Reference Guide.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting#Faceting-RangeFaceting
 */
public class FacetRangeQuery extends FacetQuery {
	public static String QUERY = FacetParams.FACET_RANGE + ".q";
	public static String QUERY_HINT = QUERY + ".hint";
	public static String QUERY_ALIAS = QUERY + ".alias";
	
	/**
	 * Builds a new {@link FacetRangeQuery}.
	 * 
	 * @param q the query.
	 * @param index the query index.
	 * @param alias the query alias.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 */
	private FacetRangeQuery(
			final String q, 
			final int index,
			final String alias,
			final SolrParams optionals,
			final SolrParams requireds) {
		super(q, index, alias, optionals, requireds);
	}
	
	/**
	 * Factory method for creating a new anonymous (not indexed) query.
	 * 
	 * @param q the query string.
	 * @param alias the query alias.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 * @return a new anonymous (not indexed) query.
	 */
	public static FacetRangeQuery newAnonymousQuery(
			final String q,
			final String alias, 
			final SolrParams optionals,
			final SolrParams required) {
		return new FacetRangeQuery(q, 0, alias, optionals, required);
	}

	/**
	 * Factory method for creating a new indexed query.
	 * 
	 * @param q the query string.
	 * @param index the index.
	 * @param optionals the incoming parameters.
	 * @param requireds the incoming required parameters.
	 * @return a new anonymous (not indexed) query.
	 */
	public static FacetRangeQuery newQuery(
			final String q,
			final int index,
			final SolrParams optionals,
			final SolrParams required) {
		return new FacetRangeQuery(q, index, null, optionals, required);
	}

	/**
	 * Returns the target field of this facet query.
	 * 
	 * @return the target field of this facet query.
	 */
	public String fieldName() {
		if (fieldName != null) {
			return fieldName;
		}
		
		return fieldName = 
				"date".equals(optionalString(QUERY_HINT)) 
					? Field.DATE_OBJECT 
					: Field.NUMERIC_OBJECT;		
	}

	@Override
	protected String aliasParameterName() {
		return QUERY_ALIAS;
	}
}