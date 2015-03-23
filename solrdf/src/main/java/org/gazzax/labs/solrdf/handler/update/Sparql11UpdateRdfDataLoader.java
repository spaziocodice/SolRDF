package org.gazzax.labs.solrdf.handler.update;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;

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
	static final int READ_BUFFER_DEFAULT_SIZE = 512;

	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		
		UpdateAction.execute(
				UpdateFactory.create(
						request.getParams().get(CommonParams.Q)),
						new SolRDFDatasetGraph(request, response));
		
	}
}	