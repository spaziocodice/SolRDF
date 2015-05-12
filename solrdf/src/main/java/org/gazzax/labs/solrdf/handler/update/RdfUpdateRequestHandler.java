package org.gazzax.labs.solrdf.handler.update;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.riot.WebContent;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.gazzax.labs.solrdf.log.MessageCatalog;

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
	@Override
	@SuppressWarnings("rawtypes")
	protected Map<String, ContentStreamLoader> createDefaultLoaders(final NamedList parameters) {
		final Map<String, ContentStreamLoader> registry = new HashMap<String, ContentStreamLoader>();
		registry.put("application/x-www-form-urlencoded", new Sparql11UpdateRdfDataLoader());
		registry.put("application/rdf+xml", new Sparql11UpdateRdfDataLoader());
		registry.put(WebContent.contentTypeSPARQLUpdate, new Sparql11UpdateRdfDataLoader());
		
		if (log.isDebugEnabled()) {
			prettyPrint(registry);
		}
		
		return registry;
	}
	
	@Override
	public String getDescription() {
		return "SPARQL 1.1 Update Request Handler";
	}
	
	@Override
	public String getSource() {
		return "https://github.com/agazzarini/SolRDF";
	}
	
	/**
	 * Debugs the registry content.
	 */
	void prettyPrint(final Map<String, ContentStreamLoader> registry) {
		for (final Entry<String, ContentStreamLoader> entry : registry.entrySet()) {
			log.debug(MessageCatalog._00098_UPDATE_HANDLER_REGISTRY_ENTRY, entry.getKey(), entry.getValue());
		}		
	}
	
}