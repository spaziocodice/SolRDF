package org.gazzax.labs.solrdf.search.qparser;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryParseException;

public class SparqlQParser extends QParser {

	SparqlQParser(
			final String qstr, 
			final SolrParams localParams,
			final SolrParams params, 
			final SolrQueryRequest req) {
		super(qstr, localParams, params, req);
	}

	@Override 
	public Query parse() throws SyntaxError {
		try {
			return new SparqlQuery(QueryFactory.create(qstr));
		} catch (final QueryParseException exception) {
			throw new SyntaxError(exception);
		}
	}
}
