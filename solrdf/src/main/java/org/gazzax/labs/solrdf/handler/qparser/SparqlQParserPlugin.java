package org.gazzax.labs.solrdf.handler.qparser;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class SparqlQParserPlugin extends QParserPlugin {

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