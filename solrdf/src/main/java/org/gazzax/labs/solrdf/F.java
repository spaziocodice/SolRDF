package org.gazzax.labs.solrdf;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.ContentStream;
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
	
	/**
	 * Reads the incoming stream in order to build the wrapped operation.
	 * 
	 * @param stream the incoming content stream.
	 * @return the incoming stream in order to build the wrapped operation.
	 * @throws IOException in case of I/O failure.
	 */
	public static String readCommandFromIncomingStream(final ContentStream stream) throws IOException {
		final BufferedReader reader = new BufferedReader(stream.getReader());
		final StringBuilder builder = new StringBuilder();
		String actLine = null;
		try {
			while ( (actLine = reader.readLine()) != null) {
				builder.append(actLine).append(" ");
			}
			return builder.toString();
		} finally {
			IOUtils.closeQuietly(reader);
		}
	}
}
