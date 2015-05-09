package org.gazzax.labs.solrdf;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A Booch Utility holding shared and global functions. 
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class F {
	/**
	 * Returns true if the query is hybrid (e.g. SPARQL with Solr parameters)
	 * 
	 * @param request the current Solr request.
	 * @return true if the query is hybrid (e.g. SPARQL with Solr parameters)
	 */
	public static boolean isHybrid(final SolrQueryRequest request) {
		return request.getParams().getBool(FacetParams.FACET, false) 
				|| request.getParams().get(CommonParams.START) != null
				|| request.getParams().get(CommonParams.ROWS) != null;
	}
}
