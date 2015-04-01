package org.gazzax.labs.solrdf.handler.search.faceting.rq;

import org.gazzax.labs.solrdf.Field;

/**
 * A stupid value object for encapsulating a facet query with all related parameters.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class FacetRangeQuery {
	public final String q;
	public final String fieldName;
	public final String alias;
	public final String start;
	public final String end;
	public final String gap;
	
	/**
	 * Builds a new {@link FacetRangeQuery}.
	 * 
	 * @param q the query.
	 * @param alias the query alias.
	 * @param hint the facet range type hint.
	 * @param start the start bound.
	 * @param end the end bound.
	 * @param gap the gap expression.
	 */
	public FacetRangeQuery(
			final String q, 
			final String alias,
			final String hint,
			final String start,
			final String end,
			final String gap) {
		this.q = q;
		this.alias = alias;
		this.fieldName = "date".equals(hint) ? Field.DATE_OBJECT : Field.NUMERIC_OBJECT;
		this.start = start;
		this.end = end;
		this.gap = gap;
	}
}