package org.gazzax.labs.solrdf.handler.search.handler;

import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.riot.WebContent;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Names;

/**
 * A RequestHandler that dispatches SPARQL 1.1 Query and Update requests across dedicated handlers.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class Sparql11SearchHandler extends RequestHandlerBase {
	static final String SEARCH_HANDLER_PARAMETER_NAME = "s";
	static final String DEFAULT_SEARCH_HANDLER_NAME = "/sparql-query";
	
	static final String UPDATE_HANDLER_PARAMETER_NAME = "u";
	static final String DEFAULT_UPDATE_HANDLER_NAME = "/sparql-update";
	
	@Override
	public void handleRequestBody(
			final SolrQueryRequest request, 
			final SolrQueryResponse response) throws Exception {
		final SolrParams parameters = request.getParams();
		final Iterable<ContentStream> contentStreams = request.getContentStreams();
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY);
		if (WebContent.contentTypeHTMLForm.equals(httpRequest.getContentType())) {
			if (isNotNullOrEmptyString(parameters.get(Names.UPDATE_PARAMETER_NAME))) {
				request.getCore().getRequestHandler(
						parameters.get(UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME))
					.handleRequest(new SolrQueryRequest() {
						
						@Override
						public void updateSchemaToLatest() {
							request.updateSchemaToLatest();
						}
						
						@Override
						public void setParams(final SolrParams params) {
							request.setParams(params);
						}
						
						@Override
						public long getStartTime() {
							return request.getStartTime();
						}
						
						@Override
						public SolrIndexSearcher getSearcher() {
							return request.getSearcher();
						}
						
						@Override
						public IndexSchema getSchema() {
							return request.getSchema();
						}
						
						@Override
						public SolrParams getParams() {
							return request.getParams();
						}
						
						@Override
						public String getParamString() {
							return request.getParamString();
						}
						
						@Override
						public SolrParams getOriginalParams() {
							return request.getOriginalParams();
						}
						
						@Override
						public SolrCore getCore() {
							return request.getCore();
						}
						
						@Override
						public Map<Object, Object> getContext() {
							return request.getContext();
						}
						
						@Override
						public Iterable<ContentStream> getContentStreams() {
							final List<ContentStream> dummyStream = new ArrayList<ContentStream>(1);
							dummyStream.add(new ContentStreamBase.ByteArrayStream(new byte[0], "dummy") {
								@Override
								public String getContentType() {
									return WebContent.contentTypeHTMLForm;
								}
							});
							
							return dummyStream;
						}
						
						@Override
						public void close() {
						}
					},  response);
			}
		} else {
			request.getCore().getRequestHandler(
					(contentStreams == null || !contentStreams.iterator().hasNext())
						? parameters.get(SEARCH_HANDLER_PARAMETER_NAME, DEFAULT_SEARCH_HANDLER_NAME) 
						: parameters.get(UPDATE_HANDLER_PARAMETER_NAME, DEFAULT_UPDATE_HANDLER_NAME))
				.handleRequest(request, response);
		}
	}

	@Override
	public String getDescription() {
		return "SPARQL 1.1 Search Handler";
	}

	@Override
	public String getSource() {
		return "https://github.com/agazzarini/SolRDF";
	}
}