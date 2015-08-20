package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

public final class NodeDocSet {
	public final int docId;
	private final Triple pattern;
	
	private final DocSet children;
	
	private NodeDocSet(final int docId, final DocSet children, final Triple pattern) {
		this.docId = docId;
		this.children = children;
		this.pattern = pattern;
	}
	
	public boolean isTopLevel() {
		return docId == -1;
	}
	
	public static NodeDocSet root(final DocSet children, final Triple pattern) {
		return new NodeDocSet(-1, children, pattern);
	}
	
	public static NodeDocSet node(final int docId, final DocSet children, final Triple pattern) {
		return new NodeDocSet(docId, children, pattern);
	}
	
	public int size() {
		return children.size();
	}
}
