
package org.gazzax.labs.solrdf.handler.search.algebra;

import static org.gazzax.labs.solrdf.NTriples.asNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.graph.standalone.LocalGraph;

import com.hp.hpl.jena.graph.Node;
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
	final static List<Binding> EMPTY_BINDINGS = Collections.emptyList();
	
    private List<Binding> bindings = new ArrayList<Binding>();
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
               
        // FIXME: solrreq--> better handling (Constants)
        final SolrQueryRequest req = (SolrQueryRequest)executionContext.getContext().get(Symbol.create("solrreq"));
		final SolrIndexSearcher searcher = (SolrIndexSearcher) req.getSearcher();
        final LocalGraph graph = (LocalGraph) executionContext.getActiveGraph() ;
//		final SizeOrderedDocSets collector = new SizeOrderedDocSets();
		final List<PatternDocSet> collector = new ArrayList<PatternDocSet>(bgp.size());
		try {
			for (final Triple triplePattern : bgp) {
				collector.add(
						new LeafPatternDocSet(
								searcher.getDocSet(
										QParser.getParser(
												graph.deleteQuery(triplePattern), "lucene", req) // FIXME: graph.deleteQuery--> TBREnamed
										.getQuery()),
								triplePattern));
	        }
			
			Collections.sort(collector, new Comparator<DocSet>() {
				@Override
				public int compare(final DocSet o1, final DocSet o2) {
					return o1.size() - o2.size();
				}
			});
			
			final Set<String> alreadyCollectedVariables = new HashSet<String>();

			// Special (simplest) case: one pattern.
			if (collector.size() == 1) {
				
				final PatternDocSet docset = collector.iterator().next();
				collectBindings(docset, searcher, alreadyCollectedVariables);
				
			} else if (collector.size() > 1){
				
				final Iterator<PatternDocSet> iterator = collector.iterator();
				
				// FIXME: appropriate naming
				PatternDocSet previousTopLevelDocSet = iterator.next();
				
				while (iterator.hasNext()) {
					final PatternDocSet nextTopLevelDocSet = iterator.next();	
					previousTopLevelDocSet = collectBindings(previousTopLevelDocSet, nextTopLevelDocSet, searcher, alreadyCollectedVariables);
				}
				collectBindings(previousTopLevelDocSet, searcher, alreadyCollectedVariables);
			}	
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		
		this.iterator = bindings.iterator();
    }

	void collectBindings(final PatternDocSet docset, final SolrIndexSearcher searcher, final Set<String> alreadyCollectedVariables) throws IOException {
		if (docset == null) { 
			bindings = EMPTY_BINDINGS;
			return;
		}
		
		final Triple pattern = docset.getTriplePattern();
		final DocIterator iterator = docset.iterator();
		while (iterator.hasNext()) { 
			final Document document = searcher.doc(iterator.nextDoc());
			final BindingMap binding = BindingFactory.create(docset.getParentBinding());
			
			collectBinding(pattern.getSubject(), binding, alreadyCollectedVariables, document, Field.S);
			collectBinding(pattern.getPredicate(), binding, alreadyCollectedVariables, document, Field.P);
			collectBinding(pattern.getObject(), binding, alreadyCollectedVariables, document, Field.O);
			
			if (!binding.isEmpty()) {
				bindings.add(binding);
			}
		}
	}
	
	private PatternDocSet collectBindings(
			final PatternDocSet first, 
			final PatternDocSet second, 
			final SolrIndexSearcher searcher, 
			final Set<String> alreadyCollectedVariables) throws IOException {
		final CompositePatternDocSet result = new CompositePatternDocSet();
		final DocIterator iterator = first.iterator();
		final Set<String> localCollectedVariables = new HashSet<String>();
		final Triple pattern = first.getTriplePattern();
		
 		while (iterator.hasNext()) {
			final Document document = searcher.doc(iterator.nextDoc());
			final BindingMap binding = BindingFactory.create(first.getParentBinding());
			final BooleanQuery query = new BooleanQuery();
			
			collectBinding(pattern.getSubject(), second.getTriplePattern().getSubject(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.S, query);
			collectBinding(pattern.getPredicate(), second.getTriplePattern().getPredicate(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.P, query);
			collectBinding(pattern.getObject(), second.getTriplePattern().getObject(), binding, alreadyCollectedVariables, localCollectedVariables, document, Field.O, query);
			
			// TODO: e se il binding vuoto???
			// TODO: se il docset è vuoto non aggiungerlo
			result.union(new LeafPatternDocSet(searcher.getDocSet(query, second), second.getTriplePattern(), binding));
		}

		alreadyCollectedVariables.addAll(localCollectedVariables);
		return result;		
	}
	
	void collectBinding(
			final Node member, 
			final BindingMap binding,
			final Set<String> alreadyCollectedVariables, 
			final Document document, 
			final String fieldName) {
		if (member.isVariable() && !alreadyCollectedVariables.contains(member.getName())) {
			binding.add(Var.alloc(member), asNode(document.get(fieldName)));
		}
	}	
	
	void collectBinding(
			final Node memberOfTheFirstPattern, 
			final Node memberOfTheSecondPattern, 
			final BindingMap binding,
			final Set<String> globalCollectedVariables, 
			final Set<String> localCollectedVariables, 
			final Document document, 
			final String fieldName, 
			final BooleanQuery query) {
		if (memberOfTheFirstPattern.isVariable()) {
			final String value = document.get(fieldName);
			if (memberOfTheFirstPattern.equals(memberOfTheSecondPattern)) {
				query.add(new TermQuery(new Term(fieldName, value)), Occur.MUST);
			}
			
			if (!globalCollectedVariables.contains(memberOfTheFirstPattern.getName())) {
				binding.add(Var.alloc(memberOfTheFirstPattern), asNode(value));
				localCollectedVariables.add(memberOfTheFirstPattern.getName());
			}
		}
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