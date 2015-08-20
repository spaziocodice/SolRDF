package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.search.Query;
import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

public class DocSetAndTriplePattern {
	public final Triple pattern;
	public final DocSet children;
	public final Query query;
	
	DocSetAndTriplePattern(final DocSet children, final Triple pattern, final Query query) {
		this.children = children;
		this.pattern = pattern;
		this.query = query;
	}
	
	public int size() {
		return children.size();
	}
}
