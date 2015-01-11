package org.gazzax.labs.solrdf.handler.update;

import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;

import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * Loads an RDF {@link ContentStream} into Solr.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class Sparql11UpdateRdfDataLoader extends ContentStreamLoader {
	final static int READ_BUFFER_DEFAULT_SIZE = 512;

	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		
		final StringWriter writer = new StringWriter(
				stream.getSize() != null 
					? stream.getSize().intValue() 
					: READ_BUFFER_DEFAULT_SIZE);
		
		IOUtils.copy(stream.getStream(), writer, "UTF-8");
		UpdateAction.execute(
				UpdateFactory.create(writer.toString()), 
				new SolRDFDatasetGraph(request, response));
	}
}	