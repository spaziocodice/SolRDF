package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.search.Filter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

public class DocSetWithTriplePattern implements DocSet {
	private final DocSet wrapped;
	public final Triple pattern;
	
	DocSetWithTriplePattern(DocSet wrapped, Triple pattern) {
		this.wrapped = wrapped;
		this.pattern = pattern;
	}

	public void add(int doc) {
		wrapped.add(doc);
	}

	public void addUnique(int doc) {
		wrapped.addUnique(doc);
	}

	public int size() {
		return wrapped.size();
	}

	public boolean exists(int docid) {
		return wrapped.exists(docid);
	}

	public DocIterator iterator() {
		return wrapped.iterator();
	}

	public long memSize() {
		return wrapped.memSize();
	}

	public DocSet intersection(DocSet other) {
		return wrapped.intersection(other);
	}

	public int intersectionSize(DocSet other) {
		return wrapped.intersectionSize(other);
	}

	public boolean intersects(DocSet other) {
		return wrapped.intersects(other);
	}

	public DocSet union(DocSet other) {
		return wrapped.union(other);
	}

	public int unionSize(DocSet other) {
		return wrapped.unionSize(other);
	}

	public DocSet andNot(DocSet other) {
		return wrapped.andNot(other);
	}

	public int andNotSize(DocSet other) {
		return wrapped.andNotSize(other);
	}

	public Filter getTopFilter() {
		return wrapped.getTopFilter();
	}

	public void addAllTo(DocSet target) {
		wrapped.addAllTo(target);
	}
	
	
}
