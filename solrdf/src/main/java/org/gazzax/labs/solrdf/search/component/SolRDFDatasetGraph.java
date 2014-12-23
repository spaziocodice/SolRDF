package org.gazzax.labs.solrdf.search.component;

import java.util.Iterator;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetGraphCaching;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * SolRDF (Solr low level) implementation of a Jena {@link Dataset}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDFDatasetGraph extends DatasetGraphCaching {
	final SolrIndexSearcher searcher;
	final SortSpec sort;
	
	/**
	 * Builds a new Dataset graph with the given factory.
	 * 
	 * @param factory the storage layer (abstract) factory.
	 */
	public SolRDFDatasetGraph(final SolrIndexSearcher searcher, final SortSpec sort) {
		this.searcher = searcher;
		this.sort = sort;
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
		return new SolRDFGraph(graphNode, searcher, sort);
	}

	@Override
	protected Graph _createDefaultGraph() {
		return new SolRDFGraph(searcher, sort);
	}

	@Override
	protected boolean _containsGraph(final Node graphNode) {
		return false;
	}

	@Override
	protected void addToDftGraph(final Node s, final Node p, final Node o) {
		getDefaultGraph().add(new Triple(s,p,o));
	}

	@Override
	protected void addToNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		// Nothing to be done here...
	}

	@Override
	protected void deleteFromDftGraph(final Node s, final Node p, final Node o) {
		getDefaultGraph().delete(new Triple(s,p,o));
	}

	@Override
	protected void deleteFromNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		// Nothing to be done here...
	}

	@Override
	protected Iterator<Quad> findInDftGraph(final Node s, final Node p, final Node o) {
		return triples2quads(Quad.tripleInQuad, getDefaultGraph().find(s, p, o));
	}

	@Override
	protected Iterator<Quad> findInSpecificNamedGraph(final Node g, final Node s, final Node p, final Node o) {
		// Nothing to be done here...
		return null;
	}

	@Override
	protected Iterator<Quad> findInAnyNamedGraphs(final Node s, final Node p, final Node o) {
		// Nothing to be done here...
		return null;
	}
}