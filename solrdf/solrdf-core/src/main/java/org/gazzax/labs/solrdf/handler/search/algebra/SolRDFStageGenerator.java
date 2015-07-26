package org.gazzax.labs.solrdf.handler.search.algebra;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Substitute;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterBlockTriples;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPeek;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;
import com.hp.hpl.jena.sparql.engine.main.StageGenerator;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderLib;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderProc;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.mgt.Explain;

public class SolRDFStageGenerator implements StageGenerator {
//	protected static final Set<String> TRIPLE_FIELDS = new HashSet<String>();
//	static {
//		TRIPLE_FIELDS.add(Field.S);
//		TRIPLE_FIELDS.add(Field.P);
//		TRIPLE_FIELDS.add(Field.O);
//	}
//	
//	private Iterator<Triple> graphIterator;
//	
//	@Override
//	public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
//		final SolrQueryRequest req = (SolrQueryRequest)execCxt.getContext().get(Symbol.create("solrreq"));
//		final SolrIndexSearcher searcher = (SolrIndexSearcher) req.getSearcher();
//		
//		LocalGraph graph = (LocalGraph) execCxt.getActiveGraph() ;
//		
//		try {
//	
//			final SizeOrderedDocSets collector = new SizeOrderedDocSets();
//	
//			for (Triple t : pattern) {
//				collector.add(searcher.getDocSet(QParser.getParser(graph.deleteQuery(t), "lucene", req).getQuery()));
//			}
//	
//			DocSet result = null;
//			final Iterator<DocSet> iterator = collector.iterator();
//	
//			if (iterator.hasNext()) {
//				final DocSet first = iterator.next();
//	
//				for (; iterator.hasNext();) {
//					final DocSet second = iterator.next();
//					result = refine(first, second, searcher, result);
//				}
//			}
//	
//			final DocIterator resultIterator = result.iterator();
//			final Iterator<Triple> triplesIterator = new UnmodifiableIterator<Triple>() {
//
//				@Override
//				public boolean hasNext() {
//					return resultIterator.hasNext();
//				}
//
//				@Override
//				public Triple next() {
//					try {
//						final Document document = searcher.doc(resultIterator.next(), TRIPLE_FIELDS);
//						return Triple.create(
//								NTriples.asURIorBlankNode((String) document.get(Field.S)), 
//								NTriples.asURI((String) document.get(Field.P)),
//								NTriples.asNode((String) document.get(Field.O)));
//					} catch (IOException exception) {
//						// FIXME: better handling
//						throw new RuntimeException(exception);
//					}
//				}
//			};
//			
//			return new QueryIteratorBase() {
//				boolean finished;
//		        private Binding slot;
//				
//				@Override
//				public void output(IndentedWriter out, SerializationContext sCxt) {
//					// TODO Auto-generated method stub
//					
//				}
//		        
//				@Override
//				protected boolean hasNextBinding() {
//					 if ( finished ) return false;    
//					 if ( slot != null ) return true ;
//					 
//					 while (triplesIterator.hasNext() && slot == null ) {
//						 Triple t = triplesIterator.next() ;
//						 slot = mapper(t) ;
//					 }
//					 
//					 if ( slot == null ) finished = true ;
//		            return slot != null ;
//				}
//
//				@Override
//				protected Binding moveToNextBinding() {
//		            if ( ! hasNextBinding() ) 
//		                throw new ARQInternalErrorException() ;
//		            Binding r = slot ;
//		            slot = null ;
//		            return r ;
//				}
//
//				@Override
//				protected void closeIterator() {
//					
//					
//				}
//
//				@Override
//				protected void requestCancel() {
//					// TODO Auto-generated method stub
//					
//				}
//				
//		        private Binding mapper(Triple r)
//		        {
//		        	return null;
////		            BindingMap results = BindingFactory.create(binding) ;
////
////		            if ( ! insert(s, r.getSubject(), results) )
////		                return null ; 
////		            if ( ! insert(p, r.getPredicate(), results) )
////		                return null ;
////		            if ( ! insert(o, r.getObject(), results) )
////		                return null ;
////		            return results ;
//		        }
//
//		        private boolean insert(Node inputNode, Node outputNode, BindingMap results)
//		        {
//		        	return true;
////		            if ( ! Var.isVar(inputNode) )
////		                return true ;
////		            
////		            Var v = Var.alloc(inputNode) ;
////		            Node x = results.get(v) ;
////		            if ( x != null )
////		                return outputNode.equals(x) ;
////		            
////		            results.add(v, outputNode) ;
////		            return true ;
//		        }
//				
//				
//			};
//			
//		} catch (Exception exception) {
//			exception.printStackTrace();
//		}
//		return null; 
//	}
//	
//	private DocSet refine(final DocSet first, final DocSet second, final SolrIndexSearcher searcher, final DocSet collector) throws IOException {
//		DocSet result = null;
//		final DocIterator outer = first.iterator();
//		while (outer.hasNext()) {
//			final Document doc = searcher.doc(outer.nextDoc());
//			final String s = doc.get("s");
//			
//			final DocSet set = searcher.getDocSet(new TermQuery(new Term("s", s)), second);
//			if (result == null) {
//				result = set;
//			} else {
//				result = result.union(set);
//			}
//		}
//		return collector != null ? collector.union(result) : result;		
//	}	
	
    private static final ReorderTransformation REORDER_FIXED = ReorderLib.fixed() ;
    
    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        return execute(pattern, REORDER_FIXED, StageBuilder.executeInline, input, execCxt) ;
    }

    protected QueryIterator execute(
    		BasicPattern pattern, 
    		final ReorderTransformation reorder, 
    		final StageGenerator execution, 
    		QueryIterator input, 
    		final ExecutionContext execCxt)
    {
        Explain.explain(pattern, execCxt.getContext()) ;

        if ( ! input.hasNext() )
            return input ;
        
        if ( reorder != null && pattern.size() >= 2 ) {
            // If pattern size is 0 or one, nothing to do.
            BasicPattern bgp2 = pattern ;

            // Try to ground the pattern
            if ( ! ( input instanceof QueryIterRoot ) ) {
                final QueryIterPeek peek = QueryIterPeek.create(input, execCxt) ;
                final Binding b = peek.peek() ;
                
                // And use this one
                input = peek ;
                bgp2 = Substitute.substitute(pattern, b) ;
                
                // ---- common
                ReorderProc reorderProc = reorder.reorderIndexes(bgp2) ;
                pattern = reorderProc.reorder(pattern) ;

            }
        }
        
        Explain.explain("Reorder/generic", pattern, execCxt.getContext()) ;
        return QueryIterBlockTriples2.create(input, pattern, execCxt) ;
    }	
}
