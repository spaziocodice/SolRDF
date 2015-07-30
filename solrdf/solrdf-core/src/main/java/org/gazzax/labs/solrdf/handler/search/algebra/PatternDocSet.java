package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public interface PatternDocSet extends DocSet {

	Triple getTriplePattern();
	
	Binding getParentBinding();
}