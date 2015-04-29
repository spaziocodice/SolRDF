package org.gazzax.labs.solrdf.graph;

import java.util.Iterator;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;

public abstract class DatasetGraphSupertypeLayer extends DatasetGraphCaching {
	protected final static GraphEventConsumer NULL_GRAPH_EVENT_CONSUMER = new GraphEventConsumer() {
		
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
	
	protected final SolrQueryRequest request;
	protected final SolrQueryResponse response;
	protected final QParser qParser;
	protected final GraphEventConsumer listener;	
	
	/**
	 * Builds a new Dataset graph with the given data.
	 * 
	 * @param request the Solr query request.
	 * @param response the Solr query response.
	 * @param qParser the (SPARQL) query parser.
	 * 
	 */
	public DatasetGraphSupertypeLayer(
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
