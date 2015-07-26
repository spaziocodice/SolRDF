
package org.gazzax.labs.solrdf.handler.search.algebra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.NTriples;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter1;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;
import com.hp.hpl.jena.sparql.util.Symbol;

// FIXME : TBRENAMED
public class QueryIterBlockTriples2 extends QueryIter1
{
    private final List<Binding> bindings = new ArrayList<Binding>();
    private final Iterator<Binding> iterator;
    
    public static QueryIterator create(final QueryIterator input,
                                       final BasicPattern pattern , 
                                       final ExecutionContext execContext) {
        return new QueryIterBlockTriples2(input, pattern, execContext) ;
    }
    
    private QueryIterBlockTriples2(
    		QueryIterator input, 
    		BasicPattern bgp, 
    		ExecutionContext executionContext) {
        super(input, executionContext) ;

 
        final SolrQueryRequest req = (SolrQueryRequest)executionContext.getContext().get(Symbol.create("solrreq"));
		final SolrIndexSearcher searcher = (SolrIndexSearcher) req.getSearcher();
        final LocalGraph graph = (LocalGraph) executionContext.getActiveGraph() ;
		final SizeOrderedDocSets collector = new SizeOrderedDocSets();
		
		try {
			for (final Triple triplePattern : bgp) {
				collector.add(
						new DocSetWithTriplePattern(
								searcher.getDocSet(
										QParser.getParser(graph.deleteQuery(triplePattern), "lucene", req).getQuery()),
								triplePattern));
	        }
		
		
			DocSet result = null;
			final Iterator<DocSet> iterator = collector.iterator();
			final Set<String> alreadyCollectedVariables = new HashSet<String>();
			
			if (iterator.hasNext()) {
				final DocSetWithTriplePattern first = (DocSetWithTriplePattern) iterator.next();
	
				Triple lastPattern = null;
				for (; iterator.hasNext();) {
					final DocSetWithTriplePattern second = (DocSetWithTriplePattern) iterator.next();
					lastPattern = second.pattern;
					result = refine(first, second, first.pattern, searcher, result, alreadyCollectedVariables, bindings);
				}
				accumulateLast(result,lastPattern, searcher, alreadyCollectedVariables, bindings);
			}	
			
			
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		
		this.iterator = bindings.iterator();
    }

	private void accumulateLast(DocSet result, Triple pattern, final SolrIndexSearcher searcher, Set<String> alreadyCollectedVariables, List<Binding> bindings) throws IOException {
		final DocIterator outer = result.iterator();
		while (outer.hasNext()) {
			final Document doc = searcher.doc(outer.nextDoc());
			final BindingMap m = BindingFactory.create();
			if (pattern.getSubject().isVariable()) {
				final String name = pattern.getSubject().getName();
				final String s = doc.get("s");
			
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getSubject()), NTriples.asNode(s));
				}
			}

			if (pattern.getPredicate().isVariable()) {
				final String name = pattern.getPredicate().getName();
				final String p = doc.get("p");
			
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getPredicate()), NTriples.asNode(p));
				}
			}

			if (pattern.getObject().isVariable()) {
				final String name = pattern.getObject().getName();
				final String o = doc.get("o");
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getObject()), NTriples.asNode(o));
				}
			}
			
			if (!m.isEmpty()) {
				bindings.add(m);
			}
		}
	}

	private DocSet refine(
			final DocSet first, 
			final DocSet second, 
			final Triple pattern, 
			final SolrIndexSearcher searcher, 
			final DocSet collector,
			final Set<String> alreadyCollectedVariables,
			final List<Binding> bindings) throws IOException {
		DocSet result = null;
		final DocIterator outer = first.iterator();
		final Set<String> localCollectedVariables = new HashSet<String>();
		while (outer.hasNext()) {
			final Document doc = searcher.doc(outer.nextDoc());
			
			final BindingMap m = BindingFactory.create();
			final BooleanQuery query = new BooleanQuery();
			
			if (pattern.getSubject().isVariable()) {
				final String name = pattern.getSubject().getName();
				final String s = doc.get("s");
				query.add(new TermQuery(new Term("s", s)), Occur.MUST);
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getSubject()), NTriples.asNode(s));
				}
				localCollectedVariables.add(name);
			}

			if (pattern.getPredicate().isVariable()) {
				final String name = pattern.getPredicate().getName();
				final String p = doc.get("p");
				query.add(new TermQuery(new Term("p", p)), Occur.MUST);
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getPredicate()), NTriples.asNode(p));
				}
				localCollectedVariables.add(name);
			}

			if (pattern.getObject().isVariable()) {
				final String name = pattern.getObject().getName();
				final String o = doc.get("o");
				query.add(new TermQuery(new Term("o", o)), Occur.MUST);
				if (!alreadyCollectedVariables.contains(name)) {
					m.add(Var.alloc(pattern.getObject()), NTriples.asNode(o));
				}
				localCollectedVariables.add(name);
			}
			
			if (!m.isEmpty()) {
				bindings.add(m);
			}
			
			final DocSet set = searcher.getDocSet(query, second);
			if (result == null) {
				result = set;
			} else {
				result = result.union(set);
			}
		}
		alreadyCollectedVariables.addAll(localCollectedVariables);
		return collector != null ? collector.union(result) : result;		
	}
    @Override
    protected boolean hasNextBinding()
    {
        return iterator.hasNext();
    }

    @Override
    protected Binding moveToNextBinding()
    {
        return iterator.next();
    }

    @Override
    protected void closeSubIterator()
    {
    }
    
    @Override
    protected void requestSubCancel()
    {
    }

    @Override
    
    protected void details(IndentedWriter out, SerializationContext sCxt)
    {
//        out.print(Utils.className(this)) ;
//        out.println() ;
//        out.incIndent() ;
//        FmtUtils.formatPattern(out, pattern, sCxt) ;
//        out.decIndent() ;
    }
}