
package org.gazzax.labs.solrdf.handler.search.algebra;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.gazzax.labs.solrdf.NTriples.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;
import org.gazzax.labs.solrdf.handler.search.algebra.tt.ExtendedQueryIterator;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.hp.hpl.jena.sparql.util.Utils;

/**
 * A {@link QueryIterator} implementation for executing {@link BasicPattern}s.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class QueryIterBasicGraphPattern2 extends QueryIter1 {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(QueryIterBasicGraphPattern2.class));
	final static OpFilter NULL_FILTER = OpFilter.filter(null);
	
	final static List<Binding> EMPTY_BINDINGS = Collections.emptyList();
	final static DocSetAndTriplePattern EMPTY_DOCSET = new DocSetAndTriplePattern(new EmptyDocSet(), null);
	final static List<DocSetAndTriplePattern> NULL_DOCSETS = new ArrayList<DocSetAndTriplePattern>(2);
	static {
		NULL_DOCSETS.add(EMPTY_DOCSET);
		NULL_DOCSETS.add(EMPTY_DOCSET);
	}
	
    private final BasicPattern bgp;
    private OpFilter filter = NULL_FILTER;
    
    private final DocSetAndTriplePattern master;
    private final DocIterator masterIterator;
    private final List<DocSetAndTriplePattern> subsequents;
    
    /**
     * Builds a new iterator with the given data.
     * 
     * @param input the parent {@link QueryIterator}.
     * @param bgp the Basic Graph Pattern.
     * @param context the execution context.
     */
    public QueryIterBasicGraphPattern2(
    		final QueryIterator input, 
    		final BasicPattern bgp, 
    		final ExecutionContext context,
    		final OpFilter filter) {
        super(input, context) ;
        
        this.filter = filter != null ? filter : NULL_FILTER;
        this.bgp = bgp;
        final SolrQueryRequest request = (SolrQueryRequest)context.getContext().get(Names.SOLR_REQUEST_SYM);
		final SolrIndexSearcher searcher = (SolrIndexSearcher) request.getSearcher();
         
		try {
			final List<DocSetAndTriplePattern> docsets = docsets(bgp, request, (LocalGraph)context.getActiveGraph());
			master = docsets.get(0);
			masterIterator = master.children.iterator();
			subsequents = docsets.subList(1, docsets.size());
			
			/*
			1 --> 
				Q ---> 2 
					Q --->
			*/
//			NodeDocSet pivot = iterator.next();
			
//			PatternDocSet pivot = input instanceof ExtendedQueryIterator ? ((ExtendedQueryIterator)input).patternDocSet() : iterator.next();
			
//			while (iterator.hasNext()) {
//				final PatternDocSet subsequent = iterator.next();	
//				pivot = collectBindings(pivot, subsequent, searcher);
//			}
//			
//			final List<Binding> bindings = collectBindings(pivot, searcher);
//			
//			this.docset = pivot;
//			this.iterator = bindings.iterator();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			// FIXME : NULL OBJECTS
		}
    }
    	
	void collectBindings(
			final Binding parent,
			final int docId,
			final DocSetAndTriplePattern second, 
			final SolrIndexSearcher searcher) throws IOException {
		
		final Triple pattern = master.pattern;
		final Document document = searcher.doc(docId);
		final BindingMap binding = BindingFactory.create(parent);
		final BooleanQuery query = new BooleanQuery();
			
		collectBinding(pattern.getSubject(), second.pattern.getSubject(), binding, document, Field.S, query);
		collectBinding(pattern.getPredicate(), second.pattern.getPredicate(), binding, document, Field.P, query);
		collectBinding(pattern.getObject(), second.pattern.getObject(), binding, document, Field.O, query);
				
		final DocSet docset = query.clauses().isEmpty() 
			? second.children 
			: searcher.getDocSet(query, second.children); 
		

	}



	List<Binding> collectBindings(
			final PatternDocSet docset, 
			final SolrIndexSearcher searcher) throws IOException {		
		final DocIterator iterator = docset.iterator();
		final List<Binding> bindings = new ArrayList<Binding>(docset.size());
		
		while (iterator.hasNext()) { 
			final Document document = searcher.doc(iterator.nextDoc());
			final Triple pattern = docset.getTriplePattern();
			final BindingMap binding = BindingFactory.create(docset.getParentBinding());
			
			collectBinding(pattern.getSubject(), binding, document, Field.S);
			collectBinding(pattern.getPredicate(), binding, document, Field.P);
			collectBinding(pattern.getObject(), binding, document, Field.O);
			
			if (!binding.isEmpty()) {
				bindings.add(binding);
			}
		}
		
		return bindings;
	}
	
	void collectBinding(
			final Node member, 
			final BindingMap binding,
			final Document document, 
			final String fieldName) {
		if (member.isVariable()) {
			final Var var = Var.alloc(member);
			if (!binding.contains(var)) {
				binding.add(var, asNode(document.get(fieldName)));
			}
		}
	}	
	
	void collectBinding(
			final Node memberOfTheFirstPattern, 
			final Node memberOfTheSecondPattern, 
			final BindingMap binding,
			final Document document, 
			final String fieldName, 
			final BooleanQuery query) {
		if (memberOfTheFirstPattern.isVariable() || memberOfTheSecondPattern.isVariable()) {
			final String value = document.get(fieldName);
			if ( (memberOfTheFirstPattern.isVariable() && memberOfTheFirstPattern.equals(memberOfTheSecondPattern))) {
				query.add(new TermQuery(new Term(fieldName, value)), Occur.MUST);				
			} else if (memberOfTheSecondPattern.isVariable() && binding.contains(Var.alloc(memberOfTheSecondPattern))) {
				query.add(new TermQuery(new Term(fieldName, asNt(binding.get(Var.alloc(memberOfTheSecondPattern))))), Occur.MUST);
			}
			
			if (memberOfTheFirstPattern.isVariable()) {
				final Var var = Var.alloc(memberOfTheFirstPattern);
				if (!binding.contains(var)) {
					binding.add(var, asNode(value));
				}
			}
		}
	}		
	
	/**
	 * Returns the list of {@link PatternDocSet} coming from the execution of all triple patterns with the BGP.
	 * 
	 * @param bgp the basic graph pattern.
	 * @param request the current Solr request.
	 * @param graph the active graph.
	 * @return the list of {@link PatternDocSet} coming from the execution of all triple patterns with the BGP.
	 */
	List<DocSetAndTriplePattern> docsets(final BasicPattern bgp, final SolrQueryRequest request, final LocalGraph graph) {
		final List<DocSetAndTriplePattern> docsets = bgp.getList().parallelStream()
				.map(triplePattern ->  {
					try {
						final BooleanQuery query = new BooleanQuery();
						query.add(
								QParser.getParser(
										graph.luceneQuery(triplePattern), 
										Names.SOLR_QPARSER, 
										request)
										.getQuery(), 
								Occur.MUST);
						
						// TODO : To be removed. Expr can be more more complex. A general mapping between this and Solr is needed.
						for (Iterator<Expr> expressions = filter.getExprs().iterator(); expressions.hasNext();) {
							final Expr expression = expressions.next(); 
							if (expression.isFunction()) {
								ExprFunction function = (ExprFunction) expression;
								ExprVar exvar = (ExprVar) function.getArg(1);
								Var var = exvar.asVar();
								if (triplePattern.getObject().isVariable() && triplePattern.getObject().equals(var)) {
									final Expr vNode = function.getArg(2);
									if (">".equals(function.getOpName())){
										query.add(NumericRangeQuery.newDoubleRange("o_n", vNode.getConstant().getDouble(), null, false, true), Occur.MUST);
									} else if ("<".equals(function.getOpName())){
										query.add(NumericRangeQuery.newDoubleRange("o_n", null, vNode.getConstant().getDouble(), true, false), Occur.MUST);
									} else {
										query.add(new TermQuery(new Term("o_s", vNode.getConstant().asUnquotedString())), Occur.MUST); 
									}
								}
							}
						}
						
						return new DocSetAndTriplePattern(request.getSearcher().getDocSet(query), triplePattern);
					} catch (final IOException exception) {
						LOGGER.error(MessageCatalog._00118_IO_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern);
					} catch (final SyntaxError exception) {
						LOGGER.error(MessageCatalog._00119_QUERY_PARSING_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern);
					} catch (final Exception exception) {
						LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern);
					}										
				})
			.sorted(comparing(DocSetAndTriplePattern::size))	
			.collect(toList());
		
//		if (LOGGER.isDebugEnabled()) {
//			final long tid = System.currentTimeMillis();
//			docsets.stream().forEach(
//					docset -> LOGGER.debug(
//								MessageCatalog._00120_BGP_EXPLAIN, 
//								tid, 
//								docset.getTriplePattern(), 
//								((LeafPatternDocSet)docset).getQuery(), 
//								docset.size()));
//		}
		
		return (docsets.size() > 0 && docsets.iterator().next().size() > 0) ? docsets : NULL_DOCSETS;
	}
	
	// Per gestire i prodotti cartesiani senza dover ricordarmi che non devo fare next sull'iteratore
	private List<Binding> preparedBindings;
	
    @Override
    protected boolean hasNextBinding() {
        while (masterIterator.hasNext()) {
        	final int docId = masterIterator.next();
        	final Triple mainPattern = master.pattern;
        	
        	
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        return iterator.next();
    }

    @Override
    protected void closeSubIterator() {
    }
    
    @Override
    protected void requestSubCancel() {
    }

    @Override
    protected void details(final IndentedWriter out, final SerializationContext context) {
        out.print(Utils.className(this)) ;
        out.println() ;
        out.incIndent() ;
        FmtUtils.formatPattern(out, bgp, context) ;
        out.decIndent() ;
    }
}