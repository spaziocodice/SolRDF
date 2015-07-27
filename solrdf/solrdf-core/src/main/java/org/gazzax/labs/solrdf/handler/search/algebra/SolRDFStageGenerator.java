package org.gazzax.labs.solrdf.handler.search.algebra;

import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Substitute;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPeek;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterRoot;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;
import com.hp.hpl.jena.sparql.engine.main.StageGenerator;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderLib;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderProc;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.mgt.Explain;

/**
 * SolRDF custom basic graph pattern (BGP) executor.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDFStageGenerator implements StageGenerator {
    private static final ReorderTransformation REORDER_FIXED = ReorderLib.fixed() ;
    
    @Override
    public QueryIterator execute(final BasicPattern pattern, final QueryIterator input, final ExecutionContext execCxt) {
        return execute(pattern, REORDER_FIXED, StageBuilder.executeInline, input, execCxt) ;
    }
    
    /**
     * Executes the given {@link BasicPattern}.
     * 
     * @param pattern the Basic Graph Pattern (BGP).
     * @param reorder 
     * @param execution
     * @param input
     * @param execCxt
     * @return
     */
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
