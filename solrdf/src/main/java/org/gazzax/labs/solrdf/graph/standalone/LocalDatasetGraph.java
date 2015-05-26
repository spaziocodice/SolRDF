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
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * A local SolRDF (Solr low level) implementation of a Jena Dataset.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LocalDatasetGraph extends DatasetGraphCaching {
	final static Log LOGGER = new Log(LoggerFactory.getLogger(LocalDatasetGraph.class));
	final static SolrIndexSearcher.QueryCommand GET_GRAPH_NODES_QUERY = new SolrIndexSearcher.QueryCommand();
	static {
		GET_GRAPH_NODES_QUERY.setQuery(new MatchAllDocsQuery());
		GET_GRAPH_NODES_QUERY.setLen(0);
		GET_GRAPH_NODES_QUERY.setFlags(GET_GRAPH_NODES_QUERY.getFlags() | SolrIndexSearcher.GET_DOCSET);
	}
	
	final static SolrParams GET_GRAPH_NODES_QUERY_PARAMS = new ModifiableSolrParams().add(FacetParams.FACET_MINCOUNT, "1");
	
	final static GraphEventConsumer NULL_GRAPH_EVENT_CONSUMER = new GraphEventConsumer() {
		
		@Override
		public boolean requireTripleBuild() {
			return true;
		}
		
		@Override
		public void onDocSet(final DocSet docSet) {
			// Nothing to be done here.
		}
		
		@Override
		public void afterTripleHasBeenBuilt(final Triple triple, final int docId) {
			// Nothing to be done here.
		}
	};
	
	final SolrQueryRequest request;
	final SolrQueryResponse response;
	final QParser qParser;
	final GraphEventConsumer listener;
	
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
		this.request = request;
		this.response = response;
		this.qParser = qParser;
		this.listener = listener != null ? listener : NULL_GRAPH_EVENT_CONSUMER;
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
			final NamedList<Integer> list = facets.getTermCounts(Field.C, result.getDocSet());
			final List<Node> graphs = new ArrayList<Node>();
			for (final Entry<String, Integer> entry : list) {
				if (!LocalGraph.UNNAMED_GRAPH_PLACEHOLDER.equals(entry.getKey())) {
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
	protected void _close() {
		// Nothing to be done here...
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

	@Override
	protected void addToDftGraph(final Node s, final Node p, final Node o) {
		getDefaultGraph().add(new Triple(s, p, o));
	}

	@Override
	protected void addToNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		getGraph(g).add(Triple.create(s, p, o));
	}

	@Override
	protected void deleteFromDftGraph(final Node s, final Node p, final Node o) {
		getDefaultGraph().delete(new Triple(s, p, o));
	}

	@Override
	protected void deleteFromNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		if (containsGraph(g)) {
			getGraph(g).delete(Triple.createMatch(s, p, o));
		}
	}

	@Override
	protected Iterator<Quad> findInDftGraph(final Node s, final Node p, final Node o) {
		return triples2quads(Quad.tripleInQuad, getDefaultGraph().find(s, p, o));
	}

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		return triples2quads(g, getGraph(g).find(s, p, o));
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(final Node s, final Node p, final Node o) {
		return triples2quads(Quad.tripleInQuad, getDefaultGraph().find(s, p, o));
	}
}