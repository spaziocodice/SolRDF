package org.gazzax.labs.solrdf.handler.search.algebra;

import java.util.Iterator;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIter;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIteratorCaching;

public class QueryIteratorOptional extends QueryIter {

    private QueryIterator leftInput ; 
    private QueryIterator rightInput ;
    
	public QueryIteratorOptional(QueryIterator left, QueryIterator right, ExecutionContext execCxt) {
		super(execCxt);
		this.leftInput = left;
		this.rightInput = right;
	}

	@Override
	protected boolean hasNextBinding() {
		return leftInput.hasNext();
	}

	@Override
	protected Binding moveToNextBinding() {
		// FIXME : il right , quando trova qualcosa, dovrebbe rimuovere il binding, di modo che la prox volta l'iterazione è + corta
		Binding left = leftInput.nextBinding();
		Iterator<Var> vars = left.vars();
		while (rightInput.hasNext()) {
			final Binding right = rightInput.nextBinding();
			while (vars.hasNext()) {
				Var var = vars.next();  
				if (right.contains(var) && right.get(var).equals(left.get(var)) ) {
					rightInput = QueryIteratorCaching.reset(rightInput);
					return BindingUtils.merge(left, right);
				}
			}
		}
		rightInput = QueryIteratorCaching.reset(rightInput);
		return left;
	}

	@Override
	protected void closeIterator() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void requestCancel() {
		// TODO Auto-generated method stub
		
	}
}
