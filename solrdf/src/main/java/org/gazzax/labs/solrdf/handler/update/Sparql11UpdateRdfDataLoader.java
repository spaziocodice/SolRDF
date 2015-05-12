package org.gazzax.labs.solrdf.handler.update;

import static org.gazzax.labs.solrdf.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.gazzax.labs.solrdf.log.MessageFactory;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * Loads A SPARQL 1.1 Update {@link ContentStream} into Solr.
 * 
 * SPARQL Update is a W3C standard for an RDF update language with SPARQL syntax. 
 * 
 * @see http://www.w3.org/TR/sparql11-update
 * @author Andrea Gazzarini
 * @since 1.0
 */
class Sparql11UpdateRdfDataLoader extends ContentStreamLoader {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(Sparql11UpdateRdfDataLoader.class));

	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		
		// This is something that should happen for request coming from a Graph Store Protocol Handler.
		// However, as described there in the GSP Handler, PUT requests aren't handled in SolRDF so we won't never enter in this 
		// conditional statements.
		String q = request.getParams().get(CommonParams.Q);
		if (isNullOrEmpty(q)) {
			final BufferedReader reader = new BufferedReader(stream.getReader());
			final StringBuilder builder = new StringBuilder();
			String actLine = null;
			try {
				while ( (actLine = reader.readLine()) != null) {
					builder.append(actLine).append(" ");
				}
				q = builder.toString();
			} finally {
				IOUtils.closeQuietly(reader);
			}
		}
		
		try {
			UpdateAction.execute(
					UpdateFactory.create(URLDecoder.decode(q, characterEncoding(request))), 
					new LocalDatasetGraph(request, response));
		} catch (final QueryParseException exception) {	
			final String message = MessageFactory.createMessage(MessageCatalog._00099_INVALID_UPDATE_QUERY, q);
			LOGGER.error(message, exception);
			throw new SolrException(ErrorCode.BAD_REQUEST, message);
		}		
	}
	
	/**
	 * Returns the character encoding that will be used to decode the incoming request.
	 * 
	 * @param request the current Solr request.
	 * @return the character encoding that will be used to decode the incoming request.
	 */
	String characterEncoding(final SolrQueryRequest request) {
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY);
		return httpRequest.getCharacterEncoding() != null ? httpRequest.getCharacterEncoding() : "UTF-8";
	}
}	 