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
    		final ExecutionContext execCxt) {
        Explain.explain(pattern, execCxt.getContext()) ;

        if (! input.hasNext() ) {
        	return input ;
        }
        
        if (reorder != null && pattern.size() >= 2) {
            if (!(input instanceof QueryIterRoot)) {
                final QueryIterPeek peek = QueryIterPeek.create(input, execCxt) ;
                final Binding binding = peek.peek() ;
                
                input = peek ;
                
                pattern = reorder.reorderIndexes(
                		Substitute.substitute(pattern, binding))
                		.reorder(pattern) ;

            }
        }
        
        Explain.explain("Reorder/generic", pattern, execCxt.getContext()) ;
        return new QueryIterBasicGraphPattern2(input, pattern, execCxt) ;
    }	
}