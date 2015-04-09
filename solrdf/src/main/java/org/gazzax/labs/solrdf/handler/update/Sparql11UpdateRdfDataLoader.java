package org.gazzax.labs.solrdf.handler.update;

import static org.gazzax.labs.solrdf.Strings.isNullOrEmpty;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

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
		
		final String q = request.getParams().get(CommonParams.Q);
		if (isNullOrEmpty(q)) {
			LOGGER.error(MessageCatalog._00099_INVALID_UPDATE_QUERY);
			throw new SolrException(ErrorCode.BAD_REQUEST, MessageCatalog._00099_INVALID_UPDATE_QUERY);
		}
		
		UpdateAction.execute(
				UpdateFactory.create(q), 
				new SolRDFDatasetGraph(request, response));
	}
}	