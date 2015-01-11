package org.gazzax.labs.solrdf.handler.update;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.riot.WebContent;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;

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
		registry.put(WebContent.contentTypeSPARQLUpdate, new Sparql11UpdateRdfDataLoader());
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