package org.gazzax.labs.solrdf.search.qparser;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/**
 * A simple Lucene wrapper for a Jena Query.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlQuery extends Query {
	final com.hp.hpl.jena.query.Query query;
	
	/**
	 * Builds a new query with the given data.
	 * 
	 * @param query the wrapped query.
	 */
	public SparqlQuery(final com.hp.hpl.jena.query.Query query) {
		this.query = query;
	}
	
	/**
	 * Returns the wrapped Jena {@link com.hp.hpl.jena.query.Query}.
	 * 
	 * @return the wrapped Jena {@link com.hp.hpl.jena.query.Query}.
	 */
	public com.hp.hpl.jena.query.Query getQuery() {
		return query;
	}
	
	@Override
	public String toString(final String field) {
		return query.toString();
	}
	
	@Override
	public Weight createWeight(final IndexSearcher searcher) throws IOException {
		return new Weight() {
			
			@Override
			public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
				// TODO
				return null;
			}
			
			@Override
			public void normalize(final float norm, final float topLevelBoost) {
				// TODO 
			}
			
			@Override
			public float getValueForNormalization() throws IOException {
				// TODO
				return 0;
			}
			
			@Override
			public Query getQuery() {
				return SparqlQuery.this;
			}
			
			@Override
			public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
				// TODO 
				return null;
			}
		};
	}
}