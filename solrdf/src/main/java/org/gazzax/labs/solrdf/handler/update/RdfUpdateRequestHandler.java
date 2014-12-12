package org.gazzax.labs.solrdf.handler.update;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;

/**
 * A subclass of {@link UpdateRequestHandler} specific for RDF formats.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RdfUpdateRequestHandler extends UpdateRequestHandler {
	@Override
	@SuppressWarnings("rawtypes")
	protected Map<String, ContentStreamLoader> createDefaultLoaders(final NamedList parameters) {
		final Map<String, ContentStreamLoader> registry = new HashMap<String, ContentStreamLoader>();
		final ContentStreamLoader loader = new RdfDataLoader();
		for (final Lang language : RDFLanguages.getRegisteredLanguages()) {
			registry.put(language.getContentType().toHeaderString(), loader);
		}
		return registry;
	}
	
	@Override
	public String getDescription() {
		return "RDFUpdateRequestHandler";
	}
	
	@Override
	public String getSource() {
		return "$https://github.com/agazzarini/SolRDF/blob/master/solrdf/src/main/java/org/gazzax/labs/solrdf/handler/update/RdfUpdateRequestHandler.java $";
	}
}