package org.gazzax.labs.solrdf.search.qparser;

import org.apache.lucene.search.Query;

public class SparqlQuery extends Query {

	final com.hp.hpl.jena.query.Query query;
	
	public SparqlQuery(com.hp.hpl.jena.query.Query query) {
		this.query = query;
	}

	@Override
	public String toString(String field) {
		return query.toString();
	}
}
