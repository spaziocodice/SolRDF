package org.gazzax.labs.solrdf.search.qparser;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * SPARQL {@link QParserPlugin}.
 * Acts as a factory and configuration point for SPARQL {@link QParser}s.
 * 
 * @author Andrea Gazzarini
 * @sice 1.0
 */
public class SparqlQParserPlugin extends QParserPlugin {
	@Override
	public void init(@SuppressWarnings("rawtypes") final NamedList args) {
		// Nothing to be done at the momyny
	}

	@Override
	public QParser createParser(
			final String qstr, 
			final SolrParams localParams,
			final SolrParams params, 
			final SolrQueryRequest req) {
		return new SparqlQParser(qstr, localParams, params, req);
	}
}