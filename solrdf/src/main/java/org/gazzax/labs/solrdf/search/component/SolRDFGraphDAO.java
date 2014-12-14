package org.gazzax.labs.solrdf.search.component;

import static org.gazzax.labs.solrdf.NTriples.asNt;
import static org.gazzax.labs.solrdf.NTriples.asNtURI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.Strings;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;

/**
 * {@link GraphDAO} implementation for SolRDF.
 * 
 * @see http://lucene.apache.org/solr
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDFGraphDAO {
	final SolrIndexSearcher searcher;
	final SortSpec sort;
	private final Node name;
	
	/**
	 * Builds a new {@link GraphDAO} with the given SOLR searcher.
	 * 
	 * @param searcher the SOLR proxy that will be used for issuing queries.
	 */
	public SolRDFGraphDAO(final SolrIndexSearcher searcher, final SortSpec sort) {
		this(searcher, null, sort);
	}
	
	/**
	 * Builds a new {@link GraphDAO} with the given SOLR client.
	 * 
	 * @param searcher the SOLR proxy that will be used for issuing queries.
	 * @param name the name of the graph associated with this DAO.
	 */
	public SolRDFGraphDAO(final SolrIndexSearcher searcher, final Node name, final SortSpec sort) {
		this.searcher = searcher;
		this.name = name;
		this.sort = sort;
	}

	public void insertTriple(final Triple triple) {
		throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
	}

	public void deleteTriple(final Triple triple) {
		throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
	}
	
	public List<Triple> deleteTriples(final Iterator<Triple> triples) {
		throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
	}

	public void executePendingMutations() {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
	}
	
	public Iterator<Triple> query(final TripleMatch query) {
	    final SolrIndexSearcher.QueryCommand cmd = new SolrIndexSearcher.QueryCommand();
	    cmd.setQuery(new MatchAllDocsQuery());
	    cmd.setSort(sort.getSort());
	    cmd.setLen(10);
	    
	    final List<Query> q = new ArrayList<Query>();
	    
		final Node s = query.getMatchSubject();
		final Node p = query.getMatchPredicate();
		final Node o = query.getMatchObject();
		
		if (s != null) {
			q.add(new TermQuery(new Term(Field.S, asNt(s))));
		}
		
		if (p != null) {
			q.add(new TermQuery(new Term(Field.P, asNt(p))));
		}
		
		if (o != null) {
			if (o.isLiteral()) {
				final String language = o.getLiteralLanguage();
				if (Strings.isNotNullOrEmptyString(language)) {
					q.add(new TermQuery(new Term(Field.LANG, language)));
				}
				
				final String literalValue = o.getLiteralLexicalForm(); 
				final RDFDatatype dataType = o.getLiteralDatatype();
				if (dataType != null) {
					final String uri = dataType.getURI();
					if (XSDDatatype.XSDboolean.getURI().equals(uri)) {
						q.add(new TermQuery(new Term(Field.BOOLEAN_OBJECT, literalValue)));
					} else if (
							XSDDatatype.XSDint.getURI().equals(uri) ||
							XSDDatatype.XSDinteger.getURI().equals(uri) ||
							XSDDatatype.XSDdecimal.getURI().equals(uri) ||
							XSDDatatype.XSDdouble.getURI().equals(uri) ||
							XSDDatatype.XSDlong.getURI().equals(uri)) {
						q.add(new TermQuery(new Term(Field.NUMERIC_OBJECT, literalValue)));
					} else if (
							XSDDatatype.XSDdateTime.equals(uri) || 
							XSDDatatype.XSDdate.equals(uri)) {
						q.add(new TermQuery(new Term(Field.DATE_OBJECT, literalValue)));
					} else {
						q.add(new TermQuery(new Term(Field.TEXT_OBJECT, literalValue)));
					}
				} else {
					q.add(new TermQuery(new Term(Field.TEXT_OBJECT, literalValue)));
				}				
			} else {
				q.add(new TermQuery(new Term(Field.TEXT_OBJECT, asNt(o))));			
			}
		}
		
		if (name != null) {
			q.add(new TermQuery(new Term(Field.C, asNtURI(o))));				
		}
		
		cmd.setFilterList(q);

	    return new DeepPagingIterator(searcher, cmd, sort);
	}
	
	public long countTriples() throws IOException {
		return searcher.search(new MatchAllDocsQuery(), 0).totalHits;
	} 	
}