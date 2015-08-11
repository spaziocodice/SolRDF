package org.gazzax.labs.solrdf.removeme;

import java.io.IOException;

import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;

public interface Reducer<I extends Op> {
	Reducer<I> apply(OpFilter filter);
	PatternDocSet reduce(I input, ExecutionContext context) throws IOException, SyntaxError;
}