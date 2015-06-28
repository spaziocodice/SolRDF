package org.gazzax.labs.solrdf.search.qparser;

import static org.gazzax.labs.solrdf.F.isHybrid;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.Syntax;

/**
 * A SPARQL Query Parser.
 * That actually delegates the parsing logic to Jena parser.
 * 
 * @author Andrea gazzarini
 * @since 1.0
 */
public class SparqlQParser extends QParser {
	/**
	 * Builds a new {@link QParser} with the given data.
	 * 
	 * @param qstr the query string.
	 * @param localParams the local parameters in the request.
	 * @param params the request parameters.
	 * @param req the current Solr request.
	 */
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
			return new SparqlQuery(QueryFactory.create(qstr, Syntax.syntaxARQ), isHybrid(req));
		} catch (final Exception exception) {
			throw new SyntaxError(exception);
		}
	}
}