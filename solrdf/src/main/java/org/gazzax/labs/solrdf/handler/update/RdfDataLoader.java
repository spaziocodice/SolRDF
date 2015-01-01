package org.gazzax.labs.solrdf.handler.update;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;

import com.hp.hpl.jena.graph.Triple;

/**
 * Loads an RDF {@link ContentStream} into Solr.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
class RdfDataLoader extends ContentStreamLoader {
	final ExecutorService executor = Executors.newSingleThreadExecutor();
	
	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		
		final PipedRDFIterator<Triple> iterator = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iterator);
		
		final Runnable parser = new Runnable() {
			@Override
			public void run() {
				try {
					RDFDataMgr.parse(
							inputStream, 
							stream.getStream(), 
							RDFLanguages.contentTypeToLang(stream.getContentType()));
				} catch (final IOException exception) {
					exception.printStackTrace();
				}
			}
		};

		executor.submit(parser);
	
		final SolRDFGraph graph = SolRDFGraph.writableGraph(null, request, response);
		while (iterator.hasNext()) {
			graph.add(iterator.next());
		}
	}
}