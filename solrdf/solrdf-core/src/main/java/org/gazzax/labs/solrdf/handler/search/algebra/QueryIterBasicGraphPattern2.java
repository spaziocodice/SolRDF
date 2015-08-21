
package org.gazzax.labs.solrdf.handler.search.algebra;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.gazzax.labs.solrdf.NTriples.asNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;
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
	final static DocSetAndTriplePattern EMPTY_DOCSET = new DocSetAndTriplePattern(new EmptyDocSet(), null, null);
	final static List<DocSetAndTriplePattern> NULL_DOCSETS = new ArrayList<DocSetAndTriplePattern>(2);
	static {
		NULL_DOCSETS.add(EMPTY_DOCSET);
		NULL_DOCSETS.add(EMPTY_DOCSET);
	}
	
    private final BasicPattern bgp;
    private OpFilter filter = NULL_FILTER;
    
    private DocSetAndTriplePattern master;
    private DocIterator masterIterator;
    private List<DocSetAndTriplePattern> subsequents;
    private Iterator<DocSetAndTriplePattern> dstpIterator;
    
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
         
		try {
			final List<DocSetAndTriplePattern> docsets = docsets(
					bgp, 
					(SolrQueryRequest)context.getContext().get(Names.SOLR_REQUEST_SYM),
					(LocalGraph)context.getActiveGraph());
			master = docsets.get(0);
			masterIterator = master.children.iterator();
			subsequents = docsets.subList(1, docsets.size());
			dstpIterator = subsequents.iterator();
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			master = EMPTY_DOCSET;
			masterIterator = master.children.iterator();
			subsequents = Collections.emptyList();
			dstpIterator = subsequents.iterator();
		}
    }
    	
    private List<Binding> bindings = new ArrayList<Binding>();
    private Iterator<Binding> bindingsIterator;
    
	void join(
			final Binding parentBinding,
			final int docId,
			final Triple currentTriplePattern,
			final DocSetAndTriplePattern beforeJoin, 
			final SolrIndexSearcher searcher) throws IOException {
		
		final BindingMap binding = parentBinding != null 
				? BindingFactory.create(parentBinding) 
				: BindingFactory.create();
		
		final Document triple = searcher.doc(docId);
		collectBinding(currentTriplePattern.getSubject(), binding, triple, Field.S);
		collectBinding(currentTriplePattern.getPredicate(), binding, triple, Field.P);
		collectBinding(currentTriplePattern.getObject(), binding, triple, Field.O);
		
		
		final BooleanQuery query = query(beforeJoin.pattern, binding);
		final DocIterator iterator = 
				query.clauses().isEmpty() 
					? beforeJoin.children.iterator() 
					: searcher.getDocSet(query, beforeJoin.children).iterator();
		
		if (dstpIterator.hasNext()) {
			final DocSetAndTriplePattern next = dstpIterator.next();
			while (iterator.hasNext()) {
				join(binding, iterator.nextDoc(), beforeJoin.pattern, next, searcher);				
			}
		} else {
			while (iterator.hasNext()) {
				join(binding, iterator.nextDoc(), beforeJoin.pattern, searcher);
			}
		}
	}
	
	BooleanQuery query(final Triple pattern, final Binding binding) {
		final BooleanQuery query = new BooleanQuery();
		if (pattern.getSubject().isVariable() 
				&& binding.contains(Var.alloc(pattern.getSubject()))) {
			query.add(new TermQuery(
					new Term(
							Field.S, 
							NTriples.asNt(binding.get(Var.alloc(pattern.getSubject()))))), Occur.MUST);		
		}
		
		if (pattern.getPredicate().isVariable() 
				&& binding.contains(Var.alloc(pattern.getPredicate()))) {
			query.add(new TermQuery(
					new Term(
							Field.P, 
							NTriples.asNt(binding.get(Var.alloc(pattern.getPredicate()))))), Occur.MUST);		
		}

		if (pattern.getObject().isVariable() 
				&& binding.contains(Var.alloc(pattern.getObject()))) {
			query.add(new TermQuery(
					new Term(
							Field.O, 
							NTriples.asNt(binding.get(Var.alloc(pattern.getObject()))))), Occur.MUST);		
		}
		
		return query;
	}
	
	void join(
			final Binding parentBinding,
			final int current,
			final Triple pattern,
			final SolrIndexSearcher searcher) throws IOException {
		
		final Document triple = searcher.doc(current);
		final BindingMap binding = parentBinding != null 
				? BindingFactory.create(parentBinding) 
				: BindingFactory.create();

		collectBinding(pattern.getSubject(), binding, triple, Field.S);
		collectBinding(pattern.getPredicate(), binding, triple, Field.P);
		collectBinding(pattern.getObject(), binding, triple, Field.O);
		
		bindings.add(binding);
	}
	
	void collectBinding(
			final Node member, 
			final BindingMap binding,
			final Document triple, 
			final String fieldName) {
		if (member.isVariable()) {
			final Var var = Var.alloc(member);
			if (!binding.contains(var)) {
				binding.add(var, asNode(triple.get(fieldName)));
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
						
						return new DocSetAndTriplePattern(request.getSearcher().getDocSet(query), triplePattern, query);
					} catch (final IOException exception) {
						LOGGER.error(MessageCatalog._00118_IO_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern, null);
					} catch (final SyntaxError exception) {
						LOGGER.error(MessageCatalog._00119_QUERY_PARSING_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern, null);
					} catch (final Exception exception) {
						LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
						return new DocSetAndTriplePattern(new EmptyDocSet(), triplePattern, null);
					}										
				})
			.sorted(comparing(DocSetAndTriplePattern::size))	
			.collect(toList());
		
		if (LOGGER.isDebugEnabled()) {
			final long tid = System.currentTimeMillis();
			docsets.stream().forEach(
					data -> LOGGER.debug(
								MessageCatalog._00120_BGP_EXPLAIN, 
								tid, 
								data.pattern, 
								data.query, 
								data.children.size()));
		}
		
		return (docsets.size() > 0 && docsets.iterator().next().size() > 0) ? docsets : NULL_DOCSETS;
	}
	
    @Override
    protected boolean hasNextBinding() {
    	if (bindingsIterator != null && bindingsIterator.hasNext()) {
    		return true;
    	}
    	
        try {
	    	final SolrQueryRequest request = (SolrQueryRequest)getExecContext().getContext().get(Names.SOLR_REQUEST_SYM);
			final SolrIndexSearcher searcher = (SolrIndexSearcher) request.getSearcher();
			
	        while (masterIterator.hasNext()) {
	        	bindings.clear();
	        	final int docId = masterIterator.next();
	        	dstpIterator = subsequents.iterator(); 
	        	if (dstpIterator.hasNext()) {   
	        		join(null, docId, master.pattern, dstpIterator.next(), searcher);
	        	} else {
	        		join(null, docId, master.pattern, searcher);
	        	}
	        	
	        	if (!bindings.isEmpty()) {
	    	        bindingsIterator = bindings.iterator();	        		
	    	        return true;
	        	}
	        }
	        return false;
        } catch (final Exception exception) {
        	throw new RuntimeException(exception);
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        return bindingsIterator.next();
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