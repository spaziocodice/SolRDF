package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

public class DocSetAndTriplePattern {
	public final Triple pattern;
	public final DocSet children;
	
	DocSetAndTriplePattern(final DocSet children, final Triple pattern) {
		this.children = children;
		this.pattern = pattern;
	}
	
	public int size() {
		return children.size();
	}
}
