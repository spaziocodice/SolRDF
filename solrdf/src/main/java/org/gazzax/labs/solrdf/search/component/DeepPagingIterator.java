package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;

import com.google.common.collect.UnmodifiableIterator;
import com.hp.hpl.jena.graph.Triple;

/**
 * An iterator over SOLR results that uses the built-in Deep Paging strategy.
 * Internally it uses other iterators to represents each iteration state. 
 * 
 * @see http://solr.pl/en/2014/03/10/solr-4-7-efficient-deep-paging
 * @see http://heliosearch.org/solr/paging-and-deep-paging
 * @see <a href="http://en.wikipedia.org/wiki/Finite-state_machine">http://en.wikipedia.org/wiki/Finite-state_machine</a>
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class DeepPagingIterator extends UnmodifiableIterator<Triple> {
	protected final static Set<String> TRIPLE_FIELDS = new HashSet<String>();
	static {
		TRIPLE_FIELDS.add(Field.S);
		TRIPLE_FIELDS.add(Field.P);
		TRIPLE_FIELDS.add(Field.O);
	}

	private final SolrIndexSearcher searcher;
	final SolrIndexSearcher.QueryCommand queryCommand;
	private DocList page;
	
	private CursorMark nextCursorMark;
	private CursorMark sentCursorMark;
	
	/**
	 * Iteration state: we need to (re)execute a query. 
	 * This could be needed the very first time we start iteration and each time the current result
	 * page has been consumed.
	 */
	private final Iterator<Triple> executeQuery = new UnmodifiableIterator<Triple>() {
		@Override
		public boolean hasNext() {
			try {
			    final SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
			    searcher.search(result, queryCommand);
			    
				sentCursorMark = queryCommand.getCursorMark();
				nextCursorMark = result.getNextCursorMark();
				
				page = result.getDocListAndSet().docList;
				return page.size() > 0;
			} catch (final Exception exception) {
				throw new RuntimeException(exception);
			}
		}

		@Override
		public Triple next() {
			currentState = iterateOverCurrentPage;
			return currentState.next();
		}
	};
			
	/**
	 * Iteration state: query has been executed and now it's time to iterate over results. 
	 */
	private final Iterator<Triple> iterateOverCurrentPage = new UnmodifiableIterator<Triple>() {
		DocIterator iterator;
		
		@Override
		public boolean hasNext() {
			if (iterator().hasNext()) {
				return true;
			} else {
				iterator = null;
				currentState = checkForConsumptionCompleteness;
				return currentState.hasNext();
			}
		}
		
		@Override
		public Triple next() {
			try {
				final Document document = searcher.doc(iterator().nextDoc(), TRIPLE_FIELDS);
				return Triple.create(
						NTriples.asURIorBlankNode((String) document.get(Field.S)), 
						NTriples.asURI((String) document.get(Field.P)),
						NTriples.asNode((String) document.get(Field.O)));
			} catch (final IOException exception) {
				throw new RuntimeException(exception);
			}
		}
		
		DocIterator iterator() {
			if (iterator == null) {
				iterator = page.iterator();	
			}
			return iterator;
			 
		}
	};

	/**
	 * Iteration state: once a page has been consumed we need to determine if another query should be issued or not. 
	 */
	private final Iterator<Triple> checkForConsumptionCompleteness = new UnmodifiableIterator<Triple>() {
		@Override
		public boolean hasNext() {
			final boolean hasNext = (page.size() == queryCommand.getLen() && !sentCursorMark.equals(nextCursorMark));
			if (hasNext) {
				queryCommand.setCursorMark(nextCursorMark);			
				currentState = executeQuery;
				return currentState.hasNext();
			}
			return false;
		}

		@Override
		public Triple next() {
			return currentState.next();
		}
	};
	
	private Iterator<Triple> currentState = executeQuery;
	
	/**
	 * Builds a new iterator with the given data.
	 * 
	 * @param searcher the Solr index searcher.
	 * @param queryCommand the query command that will be submitted.
	 */
	DeepPagingIterator(final SolrIndexSearcher searcher, final SolrIndexSearcher.QueryCommand queryCommand, final SortSpec sort) {
		this.searcher = searcher;
		this.queryCommand = queryCommand;
		this.sentCursorMark = new CursorMark(searcher.getSchema(), sort);
		this.queryCommand.setCursorMark(sentCursorMark);
	}

	@Override
	public boolean hasNext() {
		return currentState.hasNext();
	}

	@Override
	public Triple next() {
		return currentState.next();
	}
}