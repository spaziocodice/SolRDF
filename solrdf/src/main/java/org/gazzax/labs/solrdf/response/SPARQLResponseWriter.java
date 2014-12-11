package org.gazzax.labs.solrdf.response;

import java.io.IOException;
import java.io.Writer;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.restlet.data.CharacterSet;
import org.restlet.engine.io.WriterOutputStream;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

public class SPARQLResponseWriter implements QueryResponseWriter {
	@Override
	public void write(
			final Writer writer, 
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws IOException {
		QueryExecution execution = null;
		final NamedList<?> values = response.getValues();
		try {
			execution = (QueryExecution) values.get(Names.QUERY_EXECUTION);
			ResultSet rs = (ResultSet) values.get(Names.SPARQL_RESULTSET);
			if (execution != null && rs != null) {
				ResultSetFormatter.outputAsXML(
						new WriterOutputStream(writer, CharacterSet.UTF_8), 
						rs);
			}
		} finally {
			if (execution != null) {
				try {execution.close();} catch (final Exception ignore) {}
			}
		}
	}

	@Override
	public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
		return "text/xml";
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void init(final NamedList args) {
		// Nothing to be done here...
	}
}