package org.gazzax.labs.solrdf.graph.cloud;

import static org.gazzax.labs.solrdf.F.fq;
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
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.graph.DatasetGraphSupertypeLayer;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

/**
 * A Jena {@link DatasetGraph} implementaton that will be used when SolRDF is running in SolrCloud.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CloudDatasetGraph extends DatasetGraphSupertypeLayer {
	static final Log LOGGER = new Log(LoggerFactory.getLogger(CloudDatasetGraph.class));
	
	protected CloudSolrServer cloud;
	
	final static SolrQuery LIST_GRAPHS_QUERY = new SolrQuery("*:*");
	static {
		LIST_GRAPHS_QUERY.setFacet(true);
		LIST_GRAPHS_QUERY.addFilterQuery("-" + Field.C + ":" + SolRDFGraph.UNNAMED_GRAPH_PLACEHOLDER);
		LIST_GRAPHS_QUERY.addFacetField(Field.C);
		LIST_GRAPHS_QUERY.setFacetMinCount(1);
	}
	
	/**
	 * Builds a new Dataset graph with the given data.
	 * 
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 */
	public CloudDatasetGraph(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final CloudSolrServer server) {
		super(request, response, null, NULL_GRAPH_EVENT_CONSUMER);
		this.cloud = server;
	}	
	
	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return new CloudGraph(graphNode, cloud, CloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new CloudGraph(null, cloud, CloudGraph.DEFAULT_QUERY_FETCH_SIZE, listener);
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		try {
			final QueryResponse response = cloud.query(LIST_GRAPHS_QUERY);
			final FacetField graphFacetField = response.getFacetField(Field.C);
			if (graphFacetField != null && graphFacetField.getValueCount() > 0) {
				final List<Node> graphs = new ArrayList<Node>();				
				for (final FacetField.Count graphName : graphFacetField.getValues()) {
					graphs.add(NTriples.asURI(graphName.getName()));
				}
				return graphs.iterator();
			}
			return EMPTY_GRAPHS_ITERATOR;
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	
	}
	
	@Override
	protected boolean _containsGraph(final Node graphNode) {
		final SolrQuery query = new SolrQuery("*:*")
			.addFilterQuery(fq(Field.C, asNtURI(graphNode)))
			.setRows(0);
		try {
			return cloud.query(query).getResults().getNumFound() > 0;
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}			    
	}
}