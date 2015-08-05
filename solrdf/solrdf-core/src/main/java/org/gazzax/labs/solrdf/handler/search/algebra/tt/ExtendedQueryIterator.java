package org.gazzax.labs.solrdf.handler.search.algebra.tt;

import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;

import com.hp.hpl.jena.sparql.engine.QueryIterator;

public interface ExtendedQueryIterator extends QueryIterator {

	PatternDocSet patternDocSet();	
}
