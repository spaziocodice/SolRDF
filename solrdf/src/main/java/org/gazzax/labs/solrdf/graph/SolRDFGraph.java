package org.gazzax.labs.solrdf.graph;

import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * Supertype layer for all SolRDF Graphs.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class SolRDFGraph extends GraphBase {
	public static final int DEFAULT_QUERY_FETCH_SIZE = 1000;
	public static final String UNNAMED_GRAPH_PLACEHOLDER = "_";
	protected static final String NULL_LANGUAGE = "_";
	protected static final TermQuery NULL_LANGUAGE_TERM_QUERY = new TermQuery(new Term(Field.LANG, NULL_LANGUAGE));

	protected final String graphNodeStringified;
	protected final GraphEventConsumer consumer;
	protected final int queryFetchSize;
	
	/**
	 * Builds a new {@link SolRDFGraph}.
	 * 
	 * @param graphNode the graph node (null in case of default node).
	 * @param consumer the listener interested in consuming graph events.
	 * @param queryFetchSize the query fetch size.
	 */
	protected SolRDFGraph(final Node graphNode, final GraphEventConsumer consumer, final int queryFetchSize) {
		this.graphNodeStringified = (graphNode != null) ? asNtURI(graphNode) : UNNAMED_GRAPH_PLACEHOLDER;
		this.consumer = consumer;
		this.queryFetchSize = queryFetchSize;
	}
	
	@Override
	public ExtendedIterator<Triple> graphBaseFind(final Triple pattern) {	
		try {
			return WrappedIterator.createNoRemove(query(pattern));
		} catch (final SyntaxError exception) {
			logger().error(MessageCatalog._00113_NWS_FAILURE, exception);
			return new NullIterator<Triple>();
		}
	}	
	
	/**
	 * Executes a query using the given triple pattern.
	 * 
	 * @param pattern the triple pattern
	 * @return an iterator containing matching triples.
	 * @throws SyntaxError in case the query cannot be executed because syntax errors.
	 */
	protected abstract Iterator<Triple> query(final Triple pattern) throws SyntaxError;
	
	/**
	 * Returns the logger associated with this {@link Graph} implementation.
	 * 
	 * @return the logger associated with this {@link Graph} implementation.
	 */
	protected abstract Log logger();
}