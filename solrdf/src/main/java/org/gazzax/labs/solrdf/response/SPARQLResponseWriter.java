package org.gazzax.labs.solrdf.response;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
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
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Formats a SPARQL result.
 * 
 * @see http://www.w3.org/TR/rdf-sparql-XMLres
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SPARQLResponseWriter implements QueryResponseWriter {
	interface ResponseWriter {
		/**
		 * Writes out the response.
		 * 
		 * @param response the response value object.
		 * @param writer the output writer.
		 * @param execution the query execution.
		 * @param contentType determines the output format.
		 * @throws IOException in case of I/O failure-
		 */
		@SuppressWarnings("rawtypes")
		void doWrite(NamedList response, Writer writer, QueryExecution execution, String contentType) throws IOException;
	}

	private Map<Integer, ResponseWriter> writers = new HashMap<Integer, ResponseWriter>();  
	
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
			if (query == null || execution == null) {
				return;
			}
			
			final ResponseWriter responseWriter = writers.get(query.getQueryType());
			responseWriter.doWrite(values, writer, execution, accept);
		} finally {
			if (execution != null) {
				// CHECKSTYLE:OFF
				try { execution.close();} catch (final Exception ignore) {}
				// CHECKSTYLE:ON
			}
		}				
	}

	@Override
	public String getContentType(final SolrQueryRequest request, final SolrQueryResponse response) {
		return getContentType(request);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void init(final NamedList args) {
		writers.put(Query.QueryTypeAsk, new ResponseWriter() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final QueryExecution execution, 
					final String contentType) {
				Boolean askResult = response.getBooleanArg(Names.QUERY_RESULT);
				if ("text/csv".equals(contentType) || "text/plain".equals(contentType)) {
					ResultSetFormatter.outputAsCSV(askResult);
				} else if ("text/tab-separated-values".equals(contentType)) {
					ResultSetFormatter.outputAsTSV(askResult);
				} else if ("application/sparql-results+xml".equals(contentType)) {
					ResultSetFormatter.outputAsXML(askResult);
				} else if ("application/sparql-results+json".equals(contentType)) {
					ResultSetFormatter.outputAsJSON(askResult);
				} 
			}
		});

		final Lang defaultLang = ResultSetLang.SPARQLResultSetXML;
		
		writers.put(Query.QueryTypeSelect, new ResponseWriter() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final QueryExecution execution, 
					final String contentType) {
				final ResultSet resultSet = (ResultSet) response.get(Names.QUERY_RESULT);
				Lang lang = RDFLanguages.contentTypeToLang(contentType);
				if (lang == null) {
					lang = defaultLang;
				}
				ResultSetMgr.write(
					new WriterOutputStream(writer, CharacterSet.UTF_8), 
					resultSet, 
					lang);
			}
		});
		
		final ResponseWriter modelResponseWriter = new ResponseWriter() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final QueryExecution execution, 
					final String contentType) {
				final Model model = (Model) response.get(Names.QUERY_RESULT);
				RDFDataMgr.write(
						new WriterOutputStream(writer, CharacterSet.UTF_8), 
						model, 
						RDFLanguages.contentTypeToLang(contentType));
			}
		};
		
		writers.put(Query.QueryTypeDescribe, modelResponseWriter);
		writers.put(Query.QueryTypeConstruct, modelResponseWriter);
	}
	
	/**
	 * Determines the content type associated with this response writer.
	 * 
	 * The mime type is first determined using the Accept request header. 
	 * If that is null, then the text/plain is assumed as default content type.
	 * 
	 * @param request the current Solr request.
	 * @return the content type associated with this response writer.
	 */
	String getContentType(final SolrQueryRequest request) {
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get("httpRequest");
		String accept = httpRequest.getHeader(HttpHeaders.ACCEPT);
		if (accept == null) {
			accept = "text/plain";
		}
		return accept;		
	}
}