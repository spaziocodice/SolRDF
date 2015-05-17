package org.gazzax.labs.solrdf.response;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpHeaders;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.restlet.data.CharacterSet;
import org.restlet.engine.io.WriterOutputStream;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Writes out on the outgoing stream a SPARQL result.
 * 
 * @see http://www.w3.org/TR/rdf-sparql-XMLres
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class HybridResponseWriter implements QueryResponseWriter {
	/**
	 * A {@link WriterStrategy} physically writes results on the outgoing stream, according with a given query type.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	interface WriterStrategy {
		/**
		 * Writes out the response.
		 * 
		 * @param response the response value object.
		 * @param writer the output writer.
		 * @param contentType determines the output format.
		 * @throws IOException in case of I/O failure-
		 */
		@SuppressWarnings("rawtypes")
		void doWrite(NamedList response, Writer writer, String contentType) throws IOException;
	}

	/**
	 * A {@link ContentTypeChosingStrategy} is responsible for determining the right content type. 
	 * It will be execute its logic according with the incoming request and the kind of query that has been executed.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	interface ContentTypeChoiceStrategy {
		/**
		 * Returns the content type that will be associated with the response.
		 * 
		 * @param detectedMediaTypes media types which are acceptable for the response.
		 * @return the content type that will be associated with the response.
		 */
		String getContentType(final String [] detectedMediaTypes);
	}

	private final static Log LOGGER = new Log(LoggerFactory.getLogger(HybridResponseWriter.class));
	
	private Map<Integer, WriterStrategy> writers = new HashMap<Integer, WriterStrategy>();  
	private Map<String, WriterStrategy> compositeWriters = new HashMap<String, WriterStrategy>();  
	private Map<String, String> contentTypeRewrites = new HashMap<String, String>();
	
	Map<Integer, ContentTypeChoiceStrategy> contentTypeChoiceStrategies = new HashMap<Integer, ContentTypeChoiceStrategy>();  
	
	@Override
	public void write(
			final Writer writer, 
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws IOException {		
		final NamedList<?> values = response.getValues();
		final Query query = (Query)request.getContext().get(Names.QUERY);
		final QueryExecution execution = (QueryExecution)response.getValues().get(Names.QUERY_EXECUTION);
		try {
			final boolean isHybridMode = Boolean.TRUE.equals(request.getContext().get(Names.HYBRID_MODE));
			if (isHybridMode) {
				response.add(Names.SOLR_REQUEST, request);
				response.add(Names.SOLR_RESPONSE, response);
				
				final String contentType = contentTypeRewrites.get(getContentType(request, false));
				WriterStrategy strategy = compositeWriters.get(contentType);
				strategy = strategy != null ? strategy : compositeWriters.get("text/xml");
				strategy.doWrite(values, writer, contentType);
			} else {
				if (query == null || execution == null) {
					LOGGER.error(MessageCatalog._00091_NULL_QUERY_OR_EXECUTION);
					return;
				}
				writers.get(query.getQueryType()).doWrite(values, writer, getContentType(request, false));
			}
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
		final boolean isHybridMode = Boolean.TRUE.equals(request.getContext().get(Names.HYBRID_MODE));
		return isHybridMode ? contentTypeRewrites.get(getContentType(request, true)) : getContentType(request, true);
	}

	private List<String> selectContentTypes = new ArrayList<String>();
	private List<String> askContentTypes = new ArrayList<String>();
	private List<String> constructContentTypes = new ArrayList<String>();
	private List<String> describeContentTypes = new ArrayList<String>();

	/**
	 * Returns the appropriate content type chooser, according with the incoming data.  
	 * 
	 * @param queryType the query type (e.g. Ask, Select, Construct, Describe)
	 * @param configuration the response writer configuration.
	 * @param contentTypesContainer the list that will hold the supported content types for a specific query (type).
	 * @return the appropriate content type chooser, according with the incoming data.  
	 */
	@SuppressWarnings("rawtypes")
	private ContentTypeChoiceStrategy contentTypeChoiceStrategy(
			final int queryType, 
			final NamedList configuration,
			final List<String> contentTypesContainer) {
		contentTypesContainer.addAll(Arrays.asList(((String) configuration.get(String.valueOf(queryType))).split(",")));
		return new ContentTypeChoiceStrategy() {	
			@Override
			public String getContentType(final String [] detectedMediaTypes) {
				if (detectedMediaTypes == null) {
					return contentTypesContainer.iterator().next();
				}
				
				for (final String mediaType : detectedMediaTypes) {
					if (contentTypesContainer.contains(mediaType)) {
						return mediaType;
					}
				}
				return contentTypesContainer.iterator().next();
			}
		}; 
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void init(final NamedList configuration) {
		ResultSetLang.init();

		final NamedList configurationByQueryType = (NamedList) configuration.get("content-types");
		contentTypeChoiceStrategies.put(Query.QueryTypeSelect, contentTypeChoiceStrategy(Query.QueryTypeSelect, configurationByQueryType, selectContentTypes));
		contentTypeChoiceStrategies.put(Query.QueryTypeAsk, contentTypeChoiceStrategy(Query.QueryTypeAsk, configurationByQueryType, askContentTypes));
		contentTypeChoiceStrategies.put(Query.QueryTypeDescribe, contentTypeChoiceStrategy(Query.QueryTypeDescribe, configurationByQueryType, describeContentTypes));
		contentTypeChoiceStrategies.put(Query.QueryTypeConstruct, contentTypeChoiceStrategy(Query.QueryTypeConstruct, configurationByQueryType, constructContentTypes));
		
		contentTypeRewrites.put(WebContent.contentTypeResultsXML, "text/xml");
		
		writers.put(Query.QueryTypeAsk, new WriterStrategy() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final String contentType) {
				final Boolean askResult = response.getBooleanArg(Names.QUERY_RESULT);
				if (WebContent.contentTypeTextCSV.equals(contentType) || WebContent.contentTypeTextPlain.equals(contentType)) {
					ResultSetFormatter.outputAsCSV(new WriterOutputStream(writer, CharacterSet.UTF_8), askResult);
				} else if (WebContent.contentTypeTextTSV.equals(contentType)) {
					ResultSetFormatter.outputAsTSV(new WriterOutputStream(writer, CharacterSet.UTF_8), askResult);
				} else if (ResultSetLang.SPARQLResultSetXML.getHeaderString().equals(contentType)) {
					ResultSetFormatter.outputAsXML(new WriterOutputStream(writer, CharacterSet.UTF_8), askResult);
				} else if (ResultSetLang.SPARQLResultSetJSON.getHeaderString().equals(contentType)) {
					ResultSetFormatter.outputAsJSON(new WriterOutputStream(writer, CharacterSet.UTF_8), askResult);
				} 
			}
		});

		compositeWriters.put("text/xml", new WriterStrategy() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final String contentType) throws IOException {
				final HybridXMLWriter xmlw = new HybridXMLWriter(
						writer, 
						(SolrQueryRequest) response.get(Names.SOLR_REQUEST), 
						(SolrQueryResponse) response.get(Names.SOLR_RESPONSE));
				xmlw.writeResponse();
			}
		});
		
		writers.put(Query.QueryTypeSelect, new WriterStrategy() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final String contentType) {
				
				ResultSetMgr.write(
					new WriterOutputStream(writer, CharacterSet.UTF_8), 
					(ResultSet) response.get(Names.QUERY_RESULT), 
					RDFLanguages.contentTypeToLang(contentType));
			}
		});
		
		final WriterStrategy modelResponseWriter = new WriterStrategy() {
			@Override
			public void doWrite(
					final NamedList response, 
					final Writer writer, 
					final String contentType) {
				RDFDataMgr.write(
						new WriterOutputStream(writer, CharacterSet.UTF_8), 
						(Model) response.get(Names.QUERY_RESULT), 
						RDFLanguages.contentTypeToLang(contentType));
			}
		};
		
		writers.put(Query.QueryTypeDescribe, modelResponseWriter);
		writers.put(Query.QueryTypeConstruct, modelResponseWriter);
	}
	
	/**
	 * Determines the content type that will be associated with this response writer.
	 * 
	 * @param request the current Solr request.
	 * @return the content type associated with this response writer.
	 */
	String getContentType(final SolrQueryRequest request, final boolean log) {
		final Query query = (Query)request.getContext().get(Names.QUERY);
		if (query == null) {
			return null;
		}

		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY);
		final String accept = httpRequest.getHeader(HttpHeaders.ACCEPT);
		
		final String [] mediaTypes = accept != null ? accept.split(",") : null;
		String [] requestedMediaTypes = null;
		if (mediaTypes != null) {
			requestedMediaTypes = new String[mediaTypes.length];
			for (int i = 0; i < mediaTypes.length; i++) {
				requestedMediaTypes[i] = cleanMediaType(mediaTypes[i]);
			}
		}
		
		final String contentType = contentTypeChoiceStrategies.get(query.getQueryType()).getContentType(requestedMediaTypes);

		if (log) {
			LOGGER.debug(MessageCatalog._00092_NEGOTIATED_CONTENT_TYPE, query.getQueryType(), accept, contentType);
		}
		return contentType;
	}
	
	/**
	 * Removes from a given media type header all parameters.
	 * 
	 * @param mediaType the incoming media type header.
	 * @return the media type without parameters (if any).
	 */
	String cleanMediaType(final String mediaType) {
		final int parametersStartIndex = mediaType.indexOf(";");
		return parametersStartIndex == -1 ? mediaType : mediaType.substring(0, parametersStartIndex);
	}
}