package org.gazzax.labs.solrdf.graph;

import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * SolRDF (Solr low level) implementation of a Jena Dataset.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDFDatasetGraph extends DatasetGraphCaching {
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
	public SolRDFDatasetGraph(
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
	public SolRDFDatasetGraph(
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
		return namedGraphs.keys();
	}

	@Override
	protected void _close() {
		// Nothing to be done here...
	}

	@Override
	protected Graph _createNamedGraph(final Node graphNode) {
		return SolRDFGraph.readableAndWritableGraph(graphNode, request, response, qParser, listener);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return SolRDFGraph.readableAndWritableGraph(null, request, response, qParser, listener);
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
		return triples2quads(Quad.tripleInQuad, getGraph(g).find(s, p, o));
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(final Node s, final Node p, final Node o) {
		return triples2quads(Quad.tripleInQuad, getDefaultGraph().find(s, p, o));
	}
}