package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.graph.DatasetGraphSupertypeLayer;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;
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
		this(request, response, null, null);
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
		this.cloud.setDefaultCollection(request.getCore().getName());
	}

	@Override
	public void close() {
		cloud.shutdown();
	}
	
	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return new ReadOnlyCloudGraph(graphNode, cloud, ReadOnlyCloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new ReadOnlyCloudGraph(null, cloud, ReadOnlyCloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		final SolrQuery query = new SolrQuery("*:*");
		query.setFacet(true);
		query.addFacetField(Field.C);
		query.setFacetMinCount(1);
		final List<Node> graphs = new ArrayList<Node>();
		try {
			final QueryResponse response = cloud.query(query);
			final FacetField graphFacetField = response.getFacetField(Field.C);
			if (graphFacetField != null) {
				for (final FacetField.Count graphName : graphFacetField.getValues()) {
					if (!SolRDFGraph.UNNAMED_GRAPH_PLACEHOLDER.equals(graphName.getName())) {
						graphs.add(NTriples.asURI(graphName.getName()));
					}
				}
			}
			return graphs.iterator();
		} catch (final Exception exception) {
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	
	}
	
	@Override
	protected boolean _containsGraph(final Node graphNode) {
		final SolrQuery query = new SolrQuery("*:*");
		query.addFilterQuery(Field.C + ":\"" + asNtURI(graphNode) + "\"");
		query.setRows(0);
		try {
			final QueryResponse response = cloud.query(query);
			return response.getResults().getNumFound() > 0;
		} catch (final Exception exception) {
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}			    
	}
}