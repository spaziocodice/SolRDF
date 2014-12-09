package org.gazzax.labs.solrdf.search.qparser;

import org.apache.lucene.search.Query;

/**
 * A simple Lucene wrapper for a Jena Query.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlQuery extends Query {
	final com.hp.hpl.jena.query.Query query;
	
	/**
	 * Builds a new query with the given data.
	 * 
	 * @param query the wrapped query.
	 */
	public SparqlQuery(final com.hp.hpl.jena.query.Query query) {
		this.query = query;
	}
	
	public com.hp.hpl.jena.query.Query getQuery() {
		return query;
	}
	
	@Override
	public String toString(final String field) {
		return query.toString();
	}
}