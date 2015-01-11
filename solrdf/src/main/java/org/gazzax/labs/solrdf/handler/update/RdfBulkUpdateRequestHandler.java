package org.gazzax.labs.solrdf.handler.update;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * A subclass of {@link UpdateRequestHandler} specific for RDF formats.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RdfBulkUpdateRequestHandler extends UpdateRequestHandler {
	/**
	 * Loads an RDF {@link ContentStream} into Solr.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	private class RdfDataLoader extends ContentStreamLoader {
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		
		final ContentStreamLoader quadsLoader = new ContentStreamLoader() {
			@Override
			public void load(
					final SolrQueryRequest request, 
					final SolrQueryResponse response,
					final ContentStream stream, 
					final UpdateRequestProcessor processor) throws Exception {
				
				final PipedRDFIterator<Quad> iterator = new PipedRDFIterator<Quad>();
				final StreamRDF inputStream = new PipedQuadsStream(iterator);
				
				executor.submit(new Runnable() {
					@Override
					public void run() {
						try {
							RDFDataMgr.parse(
									inputStream, 
									stream.getStream(), 
									RDFLanguages.contentTypeToLang(stream.getContentType()));
						} catch (final IOException exception) {
							throw new SolrException(ErrorCode.SERVER_ERROR, exception);
						}					
					}
				});
				
				final DatasetGraph dataset = new SolRDFDatasetGraph(request, response);
				while (iterator.hasNext()) {
					dataset.add(iterator.next());
				}									
			}
		};
		
		final ContentStreamLoader triplesLoader = new ContentStreamLoader() {
			@Override
			public void load(
					final SolrQueryRequest request, 
					final SolrQueryResponse response,
					final ContentStream stream, 
					final UpdateRequestProcessor processor) throws Exception {
				
				final PipedRDFIterator<Triple> iterator = new PipedRDFIterator<Triple>();
				final StreamRDF inputStream = new PipedTriplesStream(iterator);
				
				executor.submit(new Runnable() {
					@Override
					public void run() {
						try {
							RDFDataMgr.parse(
									inputStream, 
									stream.getStream(), 
									RDFLanguages.contentTypeToLang(stream.getContentType()));
						} catch (final IOException exception) {
							throw new SolrException(ErrorCode.SERVER_ERROR, exception);
						}					
					}
				});
				
				final DatasetGraph dataset = new SolRDFDatasetGraph(request, response, null);
				final Graph defaultGraph = dataset.getDefaultGraph();
				while (iterator.hasNext()) {
					defaultGraph.add(iterator.next());
				}		
			}
		};	
		
		@Override
		public void load(
				final SolrQueryRequest request, 
				final SolrQueryResponse response,
				final ContentStream stream, 
				final UpdateRequestProcessor processor) throws Exception {
			
			final Lang lang = RDFLanguages.contentTypeToLang(stream.getContentType());
			if (lang == null) {
				throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown Content-type");
			}
			
			final ContentStreamLoader delegate = 
					(lang == Lang.NQ || lang == Lang.NQUADS || lang == Lang.TRIG)
						? quadsLoader
						: triplesLoader;
			
			delegate.load(request, response, stream, processor);
		}
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	protected Map<String, ContentStreamLoader> createDefaultLoaders(final NamedList parameters) {
		final Map<String, ContentStreamLoader> registry = new HashMap<String, ContentStreamLoader>();
		final ContentStreamLoader loader = new RdfDataLoader();
		for (final Lang language : RDFLanguages.getRegisteredLanguages()) {
			registry.put(language.getContentType().toHeaderString(), loader);
		}
		registry.put(WebContent.contentTypeSPARQLUpdate, new Sparql11UpdateRdfDataLoader());

		return registry;
	}
	
	@Override
	public String getDescription() {
		return "RDFBulkUpdateRequestHandler";
	}
	
	@Override
	public String getSource() {
		return "$https://github.com/agazzarini/SolRDF/blob/master/solrdf/src/main/java/org/gazzax/labs/solrdf/handler/update/RdfBulkUpdateRequestHandler.java $";
	}
}