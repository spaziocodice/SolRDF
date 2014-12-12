package org.gazzax.labs.solrdf.response;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpHeaders;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.restlet.data.CharacterSet;
import org.restlet.engine.io.WriterOutputStream;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * Formats a SPARQL result.
 * 
 * @see http://www.w3.org/TR/rdf-sparql-XMLres
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SPARQLResponseWriter implements QueryResponseWriter {
	
	@Override
	public void write(
			final Writer writer, 
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws IOException {
		
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get("httpRequest");
		final String accept = httpRequest.getHeader(HttpHeaders.ACCEPT);
		
		QueryExecution execution = null;
		final NamedList<?> values = response.getValues();
		try {
			final Query query = (Query)values.get(Names.QUERY);
			execution = (QueryExecution) values.get(Names.QUERY_EXECUTION);
			if (query.isSelectType()) {
				final ResultSet resultSet = (ResultSet) values.get(Names.QUERY_RESULT);
				if (execution != null && resultSet != null) {
					ResultSetMgr.write(
							new WriterOutputStream(writer, CharacterSet.UTF_8), 
							resultSet, 
							RDFLanguages.contentTypeToLang(accept));
				}
			}
		} finally {
			if (execution != null) {
				try {execution.close();} catch (final Exception ignore) {}
			}
		}
	}

	@Override
	public String getContentType(final SolrQueryRequest request, final SolrQueryResponse response) {
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get("httpRequest");
		final String accept = httpRequest.getHeader(HttpHeaders.ACCEPT);
		return accept;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void init(final NamedList args) {
		// Nothing to be done here...
	}
}