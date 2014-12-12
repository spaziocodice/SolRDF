package org.gazzax.labs.solrdf.search.component;

import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNt;
import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.asNtURI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.gazzax.labs.jena.nosql.fwk.StorageLayerException;
import org.gazzax.labs.jena.nosql.fwk.ds.GraphDAO;
import org.gazzax.labs.jena.nosql.fwk.log.Log;
import org.gazzax.labs.jena.nosql.fwk.util.Strings;
import org.gazzax.labs.jena.nosql.solr.Field;
import org.slf4j.LoggerFactory;

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
public class SolRDFGraphDAO implements GraphDAO<Triple, TripleMatch> {
	protected final Log logger = new Log(LoggerFactory.getLogger(SolRDFGraphDAO.class));
		
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

	@Override
	public void insertTriple(final Triple triple) throws StorageLayerException {
//		final SolrInputDocument document = new SolrInputDocument();
//		document.setField(Field.S, asNt(triple.getSubject()));
//		document.setField(Field.P, asNtURI(triple.getPredicate()));
//		document.setField(Field.C, name != null ? asNtURI(name) : null);
//		document.setField(Field.O, asNt(triple.getObject()));
//
//		final Node object = triple.getObject();
//		if (object.isLiteral()) {
//			final RDFDatatype dataType = object.getLiteralDatatype();
//			final Object value = object.getLiteral().getLexicalForm();
//			document.setField(Field.LANG, object.getLiteralLanguage());				
//			
//			if (dataType != null) {
//				final String uri = dataType.getURI();
//				if (XSDDatatype.XSDboolean.getURI().equals(uri)) {
//					document.setField(Field.BOOLEAN_OBJECT, value);
//				} else if (
//						XSDDatatype.XSDint.getURI().equals(uri) ||
//						XSDDatatype.XSDinteger.getURI().equals(uri) ||
//						XSDDatatype.XSDdecimal.getURI().equals(uri) ||
//						XSDDatatype.XSDdouble.getURI().equals(uri) ||
//						XSDDatatype.XSDlong.getURI().equals(uri)) {
//					document.setField(Field.NUMERIC_OBJECT, value);
//				} else if (
//						XSDDatatype.XSDdateTime.equals(uri) || 
//						XSDDatatype.XSDdate.equals(uri)) {
//					document.setField(Field.DATE_OBJECT, value);										
//				} else {
//					document.setField(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(String.valueOf(value)));								
//				}
//			} else {
//				document.setField(Field.TEXT_OBJECT, StringEscapeUtils.escapeXml(String.valueOf(value)));			
//			}
//		} else {
//			document.setField(Field.TEXT_OBJECT, asNt(triple.getObject()));			
//		}
//		
//		try {
//			indexer.add(document, addCommitWithinMsecs);
//		} catch (final Exception exception) {
//			throw new StorageLayerException(exception);
//		}
	}

	@Override
	public void deleteTriple(final Triple triple) throws StorageLayerException {
//		try {
//			indexer.deleteByQuery(deleteQuery(triple), deleteCommitWithinMsecs);
//		} catch (final Exception exception) {
//			throw new StorageLayerException(exception);
//		}
	}
	
	@Override
	public List<Triple> deleteTriples(final Iterator<Triple> triples) {
		throw new UnsupportedOperationException("Not implemented in Apache SOLR binding.");
	}

	@Override
	public void executePendingMutations() throws StorageLayerException {
//		try {
//			indexer.commit();
//		} catch (Exception exception) {
//			throw new StorageLayerException(exception);
//		}
	}
	
	@Override
	public void clear() {
//		try {
//			indexer.deleteByQuery(name == null ? "*:*" : (Field.C + ":\"" + ClientUtils.escapeQueryChars(asNtURI(name)) + "\""), deleteCommitWithinMsecs);
//		} catch (final Exception exception) {
//			logger.error(MessageCatalog._00170_UNABLE_TO_CLEAR, exception);
//		}
	}

	@Override
	public Iterator<Triple> query(final TripleMatch query) throws StorageLayerException {
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
	

	@Override
	public long countTriples() throws StorageLayerException {
		try {
			return searcher.search(new MatchAllDocsQuery(), 0).totalHits;
		} catch (final IOException exception) {
			throw new StorageLayerException(exception);
		}
	} 	
	
	/**
	 * Builds a filter query with the given data.
	 * 
	 * @param fieldName the field name.
	 * @param value the field value.
	 * @return a filter query with the given data.
	 */
	String newFilterQuery(final String fieldName, final String value, final boolean phraseQuery) {
		return new StringBuilder()
			.append(fieldName)
			.append(phraseQuery ? ":\"" : ":")
			.append(ClientUtils.escapeQueryChars(value))
			.append(phraseQuery ? "\"" : "")
			.toString();
	}
	
	/**
	 * Builds a delete query starting from a given triple.
	 * 
	 * @param triple the triple.
	 * @return a delete query starting from a given triple.
	 */
	String deleteQuery(final Triple triple) {
		
		final StringBuilder builder = new StringBuilder();
		if (triple.getSubject().isConcrete()) {
			builder.append(Field.S).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getSubject()))).append("\"");
		}
		
		if (triple.getPredicate().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(Field.P).append(":\"").append(ClientUtils.escapeQueryChars(asNtURI(triple.getPredicate()))).append("\"");
		}
			
		if (triple.getObject().isConcrete()) {
			if (builder.length() != 0) {
				builder.append(" AND ");
			}
			builder.append(Field.O).append(":\"").append(ClientUtils.escapeQueryChars(asNt(triple.getObject()))).append("\"");
		}
			
		
		if (name != null) {
			builder.append(" AND ").append(Field.C).append(":\"").append(ClientUtils.escapeQueryChars(asNtURI(name))).append("\"");
		}
		
		return builder.toString();
	}
}