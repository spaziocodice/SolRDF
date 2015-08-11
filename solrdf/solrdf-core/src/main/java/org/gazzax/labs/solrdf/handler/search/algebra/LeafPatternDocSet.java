package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.search.Filter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

/**
 * A simple {@link DocSet} wrapper that encapsulates a {@link DocSet} with a (source) {@link Triple}Pattern.
 * Each instance could also contains a binding, which is used for building the expected results. 
 * In case that binding is null, the {@link LeafPatternDocSet} is supposed to be a top level {@link DocSet}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class LeafPatternDocSet implements PatternDocSet {
	private final DocSet delegate;
	
	private final Triple pattern;
	private final Binding parentBinding;
	
	/**
	 * Builds a new top level {@link LeafPatternDocSet} with the given data.
	 * 
	 * @param docset the {@link DocSet}.
	 * @param pattern the {@link Triple}Pattern that originates the {@link DocSet} above.
	 * @param binding the parent {@link Binding}, null in case of top level {@link LeafPatternDocSet}.
	 */
	public LeafPatternDocSet(final DocSet docset, final Triple pattern, final Binding binding) {
		this.delegate = docset;
		this.pattern = pattern;
		this.parentBinding = binding;
	}
	
	/**
	 * Returns true if this is a top level {@link LeafPatternDocSet}.
	 * That is, returns true if the parent binding member is null.
	 * 
	 * @return true if this is a top level {@link LeafPatternDocSet}.
	 */
	public boolean isTopLevel() {
		return parentBinding == null;
	}
	
	/**
	 * Builds a new {@link LeafPatternDocSet} with the given data.
	 * 
	 * @param docset the {@link DocSet}.
	 * @param pattern the {@link Triple}Pattern that originates the {@link DocSet} above.
	 */
	public LeafPatternDocSet(final DocSet docset, final Triple pattern) {
		this(docset, pattern, null);
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

	@Override
	public Triple getTriplePattern() {
		return pattern;
	}

	@Override
	public Binding getParentBinding() {
		return parentBinding;
	}	
}