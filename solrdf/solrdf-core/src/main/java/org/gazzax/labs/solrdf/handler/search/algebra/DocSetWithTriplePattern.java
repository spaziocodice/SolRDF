package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.search.Filter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;

/**
 * A simple {@link DocSet} wrapper that encapsulates a {@link DocSet} with a (source) {@link Triple}Pattern.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class DocSetWithTriplePattern implements DocSet {
	private final DocSet delegate;
	public final Triple pattern;
	
	/**
	 * Builds a new {@link DocSetWithTriplePattern} with the given data.
	 * 
	 * @param docset the {@link DocSet}.
	 * @param pattern the {@link Triple}Pattern that originates the {@link DocSet} above.
	 */
	DocSetWithTriplePattern(final DocSet docset, final Triple pattern) {
		this.delegate = docset;
		this.pattern = pattern;
	}
	
	@Override
	public void add(final int doc) {
		delegate.add(doc);
	}

	@Override
	public void addUnique(final int doc) {
		delegate.addUnique(doc);
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean exists(final int docid) {
		return delegate.exists(docid);
	}

	@Override
	public DocIterator iterator() {
		return delegate.iterator();
	}

	@Override
	public long memSize() {
		return delegate.memSize();
	}

	@Override
	public DocSet intersection(final DocSet other) {
		return delegate.intersection(other);
	}

	@Override
	public int intersectionSize(final DocSet other) {
		return delegate.intersectionSize(other);
	}

	@Override
	public boolean intersects(final DocSet other) {
		return delegate.intersects(other);
	}

	@Override
	public DocSet union(final DocSet other) {
		return delegate.union(other);
	}

	@Override
	public int unionSize(final DocSet other) {
		return delegate.unionSize(other);
	}

	@Override
	public DocSet andNot(final DocSet other) {
		return delegate.andNot(other);
	}

	@Override
	public int andNotSize(final DocSet other) {
		return delegate.andNotSize(other);
	}

	@Override
	public Filter getTopFilter() {
		return delegate.getTopFilter();
	}

	@Override
	public void addAllTo(final DocSet target) {
		delegate.addAllTo(target);
	}	
}