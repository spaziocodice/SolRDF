package org.gazzax.labs.solrdf.graph;

import com.hp.hpl.jena.graph.Triple;

public interface GraphEventListener {
	public void afterTripleHasBeenBuilt(Triple triple, int docId);
}
