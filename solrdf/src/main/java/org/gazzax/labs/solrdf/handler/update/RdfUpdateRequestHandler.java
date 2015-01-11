package org.gazzax.labs.solrdf.handler.update;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.jena.riot.WebContent;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;

import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * An {@link UpdateRequestHandler} implementation for handling SPARQL updates. 
 * 
 * SPARQL Update is a W3C standard for an RDF update language with SPARQL syntax. 
 * 
 * @see http://www.w3.org/TR/sparql11-update
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RdfUpdateRequestHandler extends UpdateRequestHandler {
	/**
	 * Loads an RDF {@link ContentStream} into Solr.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	class RdfDataLoader extends ContentStreamLoader {
		final static int READ_BUFFER_DEFAULT_SIZE = 512;
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		
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
	
	@Override
	@SuppressWarnings("rawtypes")
	protected Map<String, ContentStreamLoader> createDefaultLoaders(final NamedList parameters) {
		final Map<String, ContentStreamLoader> registry = new HashMap<String, ContentStreamLoader>();
		registry.put(WebContent.contentTypeSPARQLUpdate, new RdfDataLoader());
		return registry;
	}
	
	@Override
	public String getDescription() {
		return "RDFBulkUpdateRequestHandler";
	}
	
	@Override
	public String getSource() {
		return "$https://github.com/agazzarini/SolRDF/blob/master/solrdf/src/main/java/org/gazzax/labs/solrdf/handler/update/RdfUpdateRequestHandler.java $";
	}
}