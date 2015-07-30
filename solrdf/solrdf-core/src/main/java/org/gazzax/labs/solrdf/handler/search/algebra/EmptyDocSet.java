package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.search.Filter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

/** 
 * A null-object {@link DocSet}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class EmptyDocSet implements DocSet {

	@Override
	public void add(int doc) {
		// do nothing
	}

	@Override
	public void addUnique(int doc) {
		// do nothing
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean exists(int docid) {
		return false;
	}

	@Override
	public DocIterator iterator() {
		return new DocIterator() {
			
			@Override
			public void remove() {
				// Nothing
			}
			
			@Override
			public Integer next() {
				return null;
			}
			
			@Override
			public boolean hasNext() {
				return false;
			}
			
			@Override
			public float score() {
				return 0;
			}
			
			@Override
			public int nextDoc() {
				return -1;
			}
		};
	}

	@Override
	public long memSize() {
		return 0;
	}

	@Override
	public DocSet intersection(final DocSet other) {
		return other;
	}

	@Override
	public int intersectionSize(final DocSet other) {
		return 0;
	}

	@Override
	public boolean intersects(final DocSet other) {
		return false;
	}

	@Override
	public DocSet union(final DocSet other) {
		return null;
	}

	@Override
	public int unionSize(final DocSet other) {
		return 0;
	}

	@Override
	public DocSet andNot(final DocSet other) {
		return null;
	}

	@Override
	public int andNotSize(final DocSet other) {
		return 0;
	}

	@Override
	public Filter getTopFilter() {
		return null;
	}

	@Override
	public void addAllTo(DocSet target) {
		// do nothing
	}
}
