package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.graph.DatasetGraphSupertypeLayer;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;

/**
 * A read only SolRDF implementation of a Jena Dataset.
 * This is a read only dataset graph because changes (i.e. updates and deletes) are executed using 
 * {@link DistributedUpdateProcessor}; that means each node will be responsible to apply local changes using 
 * its own {@link LocalDatasetGraph} instance.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class ReadOnlyCloudDatasetGraph extends DatasetGraphSupertypeLayer {
	protected CloudSolrServer cloud;
	
	/**
	 * Builds a new Dataset graph with the given data.
	 * 
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 */
	public ReadOnlyCloudDatasetGraph(
			final SolrQueryRequest request, 
			final SolrQueryResponse response) {
		super(request, response, null, null);
	}	
	
	/**
	 * Builds a new Dataset graph with the given data.
	 * 
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 * @param qParser the (SPARQL) query parser.
	 * 
	 */
	public ReadOnlyCloudDatasetGraph(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final QParser qParser,
			final GraphEventConsumer listener) {
		super(request, response, qParser, listener != null ? listener : NULL_GRAPH_EVENT_CONSUMER);
		this.cloud = new CloudSolrServer(request.getCore().getCoreDescriptor().getCoreContainer().getZkController().getZkServerAddress());
	}

	@Override
	public void close() {
		cloud.shutdown();
	}
	
	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return new ReadOnlyCloudGraph(graphNode, cloud, qParser, ReadOnlyCloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new ReadOnlyCloudGraph(null, cloud, qParser, ReadOnlyCloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	protected boolean _containsGraph(final Node graphNode) {
		final SolrQuery query = new SolrQuery("*:*");
		query.addFilterQuery(Field.C + ":\"" + asNtURI(graphNode) + "\"");
		query.setRequestHandler("/solr-query");
		query.setRows(0);
		try {
			final QueryResponse response = cloud.query(query);
			return response.getResults().getNumFound() > 0;
		} catch (final Exception exception) {
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}			    
	}
}