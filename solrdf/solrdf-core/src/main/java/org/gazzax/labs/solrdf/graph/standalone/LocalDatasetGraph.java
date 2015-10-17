package org.gazzax.labs.solrdf.graph.standalone;

import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.graph.DatasetGraphSupertypeLayer;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.SolRDFGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;

/**
 * A local SolRDF (Solr low level) implementation of a Jena Dataset.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LocalDatasetGraph extends DatasetGraphSupertypeLayer {
	final static Log LOGGER = new Log(LoggerFactory.getLogger(LocalDatasetGraph.class));
	final static SolrIndexSearcher.QueryCommand GET_GRAPH_NODES_QUERY = new SolrIndexSearcher.QueryCommand();
	static {
		GET_GRAPH_NODES_QUERY.setQuery(new MatchAllDocsQuery());
		GET_GRAPH_NODES_QUERY.setLen(0);
		GET_GRAPH_NODES_QUERY.setFlags(GET_GRAPH_NODES_QUERY.getFlags() | SolrIndexSearcher.GET_DOCSET);
	}
	
	final static SolrParams GET_GRAPH_NODES_QUERY_PARAMS = new ModifiableSolrParams().add(FacetParams.FACET_MINCOUNT, "1");
	
	/**
	 * Builds a new Dataset graph with the given data.
	 * 
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 */
	public LocalDatasetGraph(final SolrQueryRequest request, final SolrQueryResponse response) {
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
	public LocalDatasetGraph(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final QParser qParser,
			final GraphEventConsumer listener) {
		super(request, response, qParser, listener);
	}
	
	@Override
	public Iterator<Node> listGraphNodes() {
	    final SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
	    try {
			request.getSearcher().search(result, GET_GRAPH_NODES_QUERY);
			final SimpleFacets facets = new SimpleFacets(
		    		request, 
		    		result.getDocSet(), 
		    		GET_GRAPH_NODES_QUERY_PARAMS);
			
			final NamedList<Integer> list = facets.getFacetTermEnumCounts(
					request.getSearcher(), 
					result.getDocSet(),
					Field.C,
					0,
					-1,
					1,
					false,
					"count"
					,null,
					null,
					false,
					null);
			final List<Node> graphs = new ArrayList<Node>();
			for (final Entry<String, Integer> entry : list) {
				if (!SolRDFGraph.UNNAMED_GRAPH_PLACEHOLDER.equals(entry.getKey())) {
					graphs.add(NTriples.asURI(entry.getKey()));
				}
			}
			
			LOGGER.debug(MessageCatalog._00112_GRAPHS_TOTAL_COUNT, graphs.size());
			
			return graphs.iterator();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	    
	}

	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return LocalGraph.readableAndWritableGraph(graphNode, request, response, qParser, listener);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return LocalGraph.readableAndWritableGraph(null, request, response, qParser, listener);
	}

	@Override
	protected boolean _containsGraph(final Node graphNode) {
	    final SolrIndexSearcher.QueryCommand cmd = new SolrIndexSearcher.QueryCommand();
	    cmd.setQuery(new MatchAllDocsQuery());
	    cmd.setLen(0);
	    cmd.setFilterList(new TermQuery(new Term(Field.C, asNtURI(graphNode))));				
	    
	    final SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
	    try {
			request.getSearcher().search(result, cmd);
		    return result.getDocListAndSet().docList.matches() > 0;
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new SolrException(ErrorCode.SERVER_ERROR, exception);
		}	    
	}
}