package org.gazzax.labs.solrdf.handler.update;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.gazzax.labs.solrdf.log.MessageFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * A subclass of {@link UpdateRequestHandler} for handling RDF bulk loadings.
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
				
				final DatasetGraph dataset = new LocalDatasetGraph(request, response);
				while (iterator.hasNext()) {
					dataset.add(iterator.next());
				}									
			}
			
			@Override
			public String toString() {
				return "Quads Loader";
			};
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
		
				// Graph Store Protocol indicates the target graph URI separately.
				// So the incoming Content-type here is one that maps "Triples Loader" but
				// the indexed tuple could be a Quad.
				final String graphUri = request.getParams().get(Names.GRAPH_URI_ATTRIBUTE_NAME);
				
				final DatasetGraph dataset = new LocalDatasetGraph(request, response, null, null);
				final Graph defaultGraph = graphUri == null 
						? dataset.getDefaultGraph() 
						: dataset.getGraph(NodeFactory.createURI(graphUri));
				while (iterator.hasNext()) {
					defaultGraph.add(iterator.next());
				}		
			}
			
			@Override
			public String toString() {
				return "Triples Loader";
			};
		};	
		
		@Override
		public void load(
				final SolrQueryRequest request, 
				final SolrQueryResponse response,
				final ContentStream stream, 
				final UpdateRequestProcessor processor) throws Exception {
			
			// Default ContentStream implementation starts reading the stream and
			// if it starts with '<' then it assumes a content type of "application/xml", 
			// if it starts with '{' then it assumes a content type of "application/json" 			
			// This behaviour is wrong is SolRDF and maybe we need a custom ContentStream here
			// At the moment this is just a workaround:
			final String contentType = stream.getContentType() != null 
					&& !"application/xml".equals(stream.getContentType())
					&& !"application/json".equals(stream.getContentType()) 
						? stream.getContentType() 
						: request.getParams().get(UpdateParams.ASSUME_CONTENT_TYPE);
			
			log.debug(MessageCatalog._00094_BULK_LOADER_CT, contentType);			
						
			final Lang lang = RDFLanguages.contentTypeToLang(contentType);
			if (lang == null) {
				final String message = MessageFactory.createMessage(MessageCatalog._00095_INVALID_CT, contentType);
				log.error(message);							
				throw new SolrException(ErrorCode.BAD_REQUEST, message);
			}
			
			final ContentStreamLoader delegate = 
					(lang == Lang.NQ || lang == Lang.NQUADS || lang == Lang.TRIG)
						? quadsLoader
						: triplesLoader;
			
			log.debug(MessageCatalog._00096_SELECTED_BULK_LOADER, contentType, delegate);
			
			delegate.load(
					request, 
					response, 
					new ContentStream() {	
						@Override
						public InputStream getStream() throws IOException {
							return stream.getStream();
						}
						
						@Override
						public String getSourceInfo() {
							return stream.getSourceInfo();
						}
						
						@Override
						public Long getSize() {
							return stream.getSize();
						}
						
						@Override
						public Reader getReader() throws IOException {
							return stream.getReader();
						}
						
						@Override
						public String getName() {
							return stream.getName();
						}
						
						@Override
						public String getContentType() {
							return contentType;
						}
					}, 
				processor);
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

		if (log.isDebugEnabled()) {
			prettyPrint(registry);
		}
		
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
	
	/**
	 * Debugs the registry content.
	 */
	void prettyPrint(final Map<String, ContentStreamLoader> registry) {
		for (final Entry<String, ContentStreamLoader> entry : registry.entrySet()) {
			log.debug(MessageCatalog._00097_BULK_LOADER_REGISTRY_ENTRY, entry.getKey(), entry.getValue());
		}		
	}
}