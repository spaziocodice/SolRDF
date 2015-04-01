package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.gazzax.labs.solrdf.Field;

/**
 * A stupid value object for encapsulating a facet query with all related parameters.
 * 
 * Apart from some parameter like alias, all other parameters are described in the Solr Wiki or
 * Reference Guide.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting
 * @see https://cwiki.apache.org/confluence/display/solr/Faceting#Faceting-RangeFaceting
 */
public class FacetRangeQuery {
	public final String q;
	public final String fieldName;
	public final String alias;
	public final String start;
	public final String end;
	public final String gap;
	public final boolean hardend;
	public final String [] include;
	public final String [] other;
	public final int minCount;
	
	/**
	 * Builds a new {@link FacetRangeQuery}.
	 * 
	 * @param q the query.
	 * @param alias the query alias.
	 * @param hint the facet range type hint.
	 * @param start the start bound.
	 * @param end the end bound.
	 * @param gap the gap expression.
	 * @param hardend specifies how Solr should handle cases where the gap expression does not divide evenly between start and end bounds. 
	 * @param include bounds included or excluded?
	 * @param other it indicates if counts should also be computed for ranges that fall outside start and end.
	 * @param minCount the minimum number of occurrences required for a facet to be included in the response.
	 * @see https://cwiki.apache.org/confluence/display/solr/Faceting#Faceting-RangeFaceting
	 */
	public FacetRangeQuery(
			final String q, 
			final String alias,
			final String hint,
			final String start,
			final String end,
			final String gap,
			final boolean hardend,
			final String [] include,
			final String [] other,
			final int minCount) {
		this.q = q;
		this.alias = alias;
		this.fieldName = "date".equals(hint) ? Field.DATE_OBJECT : Field.NUMERIC_OBJECT;
		this.start = start;
		this.end = end;
		this.gap = gap;
		this.hardend = hardend;
		this.include = include;
		this.other = other;
		this.minCount = minCount;
	}
	
	/**
	 * Returns the key identifier associated with this facet range query.
	 * 
	 * @return the key identifier associated with this facet range query.
	 */
	public String key() {
		return alias != null ? alias : q;
	}
}