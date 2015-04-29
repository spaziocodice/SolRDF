package org.gazzax.labs.solrdf.graph;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.gazzax.labs.solrdf.graph.cloud.ReadOnlyCloudDatasetGraph;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;

import com.hp.hpl.jena.sparql.core.DatasetGraph;

/**
 * Factory for SolRDF {@link DatasetGraph}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class DatasetGraphFactory {
	/**
	 * Returns the {@link DatasetGraph} implementation associated with this SolRDF instance.
	 * 
	 * @param request the current Solr query request.
	 * @param request the current Solr query response.
	 * @return the {@link DatasetGraph} implementation associated with this SolRDF instance. 
	 */
	public static DatasetGraph getDatasetGraph(final SolrQueryRequest request, final SolrQueryResponse response) {
		return request.getCore().getCoreDescriptor().getCoreContainer().isZooKeeperAware() 
				? new ReadOnlyCloudDatasetGraph(request, response)
				: new LocalDatasetGraph(request, response);
	}
}