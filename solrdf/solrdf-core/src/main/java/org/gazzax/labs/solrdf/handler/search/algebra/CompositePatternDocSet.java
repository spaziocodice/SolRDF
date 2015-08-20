package org.gazzax.labs.solrdf.handler.search.algebra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.Filter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

/**
 * A compound {@link PatternDocSet} that encapsulates one or more {@link PatternDocSet}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class CompositePatternDocSet implements PatternDocSet {
	private final static DocIterator EMPTY_DOC_ITERATOR = new EmptyDocSet().iterator();
	private List<PatternDocSet> docsets = new ArrayList<PatternDocSet>();
	private int size;
	private int memSize;

	private Iterator<PatternDocSet> iterator;
	private PatternDocSet currentDocSet;
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("\n*** NEW COMPOSITE SET *** \n");
		builder.append("SIZE: ").append(size).append("\n");
		for (final PatternDocSet set : docsets) {
			builder.append("\n\t");		
			builder.append(set).append("\n");
		}
		
		return builder.toString();
	}
	
	@Override
	public Triple getTriplePattern() {
		if (currentDocSet != null) {
			return currentDocSet.getTriplePattern();
		} else {
			if (docSetIterator().hasNext()) { 
				currentDocSet = docSetIterator().next();
				return currentDocSet.getTriplePattern();
			}
			return null;
		}
	}
	
	@Override
	public Binding getParentBinding() {
		if (currentDocSet != null) {
			return currentDocSet.getParentBinding();
		} else {
			if (docSetIterator().hasNext()) {
				currentDocSet = docSetIterator().next();
				return currentDocSet.getParentBinding();
			}
			return null;
		}
	}
	
	@Override
	public void add(int doc) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addUnique(int doc) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean exists(int docid) {
		return false;
	}

	@Override
	public DocIterator iterator() {
		if (currentDocSet == null) {
			if (docSetIterator().hasNext()) {
				currentDocSet = docSetIterator().next();
			} else {
				return EMPTY_DOC_ITERATOR;
			}
		} 
		
		return new DocIterator() {
			DocIterator currentDocSetIterator = currentDocSet.iterator();

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public Integer next() {
				return currentDocSetIterator.next();
			}
			
			@Override
			public boolean hasNext() {
				if (currentDocSetIterator.hasNext()) {
					return true;
				}
				
				if (docSetIterator().hasNext()) {
					currentDocSet = docSetIterator().next();
					currentDocSetIterator = currentDocSet.iterator();
					return currentDocSetIterator.hasNext();
				}
				
				return false;
			}
			
			@Override
			public float score() {
				return currentDocSetIterator.score();
			}
			
			@Override
			public int nextDoc() {
				return currentDocSetIterator.nextDoc();
			}
		};
	}

	@Override
	public long memSize() {
		return memSize;
	}

	@Override
	public DocSet intersection(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int intersectionSize(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean intersects(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DocSet union(final DocSet other) {
		if (other.size() != 0) {
			docsets.add((LeafPatternDocSet) other);
			size += other.size();
			memSize += other.memSize();
		}
		return this;
	}

	@Override
	public int unionSize(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DocSet andNot(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int andNotSize(DocSet other) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Filter getTopFilter() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addAllTo(DocSet target) {
		throw new UnsupportedOperationException();
	}
	
	Iterator<PatternDocSet> docSetIterator() {
		if (iterator == null) {
			iterator = docsets.iterator();
		}
		return iterator;
	}	
}