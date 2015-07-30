package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
 
/**
 * Interface for a {@link DocSet} that also includes information about a pattern and an optional binding.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface PatternDocSet extends DocSet {

	/**
	 * Returns the triple patterns that originates this {@link DocSet}.
	 * 
	 * @return the triple patterns that originates this {@link DocSet}.
	 */
	Triple getTriplePattern();
	
	/**
	 * Returns the {@link Binding} associated with this {@link DocSet}.
	 * 
	 * @return the {@link Binding} associated with this {@link DocSet}.
	 */
	Binding getParentBinding();
}