package org.gazzax.labs.solrdf.graph;

import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

/**
 * An optional consumer that drives and listens the graph parsing consumption.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface GraphEventConsumer {
	/**
	 * A new triples has been built.
	 * Note that if a preceding call to {@link #requireTripleBuild()} returned false, then
	 * the given Triple can be just a placeholder ({@link DeepPagingIterator#DUMMY_TRIPLE}).
	 * 
	 * @param triple the triple.
	 * @param docId the (internal Lucene) document identifier.
	 * @see DeepPagingIterator#DUMMY_TRIPLE
	 */
	void afterTripleHasBeenBuilt(Triple triple, int docId);
	
	/**
	 * While parsing the graph the consumer can or cannot be interested in effectively building a triple.
	 * This method will be called each time a graph needs to create a new Triple representation.
	 * 
	 * For example, if we are just collecting the docIds, the consumer can control the effective triple creation
	 * therefore avoiding a lot of temporary (and unuseful) objects.
	 *  
	 * @return if the current triple match must be represented by a new {@link Triple} instance.
	 */
	boolean requireTripleBuild();

	/**
	 * The consumer is informed about the {@link DocSet} associated with the current search.
	 * This callbacks happens once per query.
	 * 
	 * @param docSet the {@link DocSet} associated with the current search.
	 */
	void onDocSet(DocSet docSet);
}