package org.gazzax.labs.solrdf.removeme;

import java.io.IOException;

import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;

public class FilterReducer implements Reducer<OpFilter> {

	@Override
	public PatternDocSet reduce(OpFilter filter, ExecutionContext context) throws IOException, SyntaxError {
		Op op = filter.getSubOp();
		// FIXME: PEZZA!
		if (op instanceof OpBGP) {
			return new BgpReducer().apply(filter).reduce((OpBGP)op, context);
		}
		return null;
	}

	@Override
	public Reducer<OpFilter> apply(OpFilter filter) {
		// TODO Auto-generated method stub
		return null;
	}

}
