package org.gazzax.labs.solrdf.handler.search.handler;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.io.IndentedWriter;
import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.tt.ExtendedQueryIterator;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;

// FIXME : wrong package 
public class BindingsQueryIterator implements QueryIterator, ExtendedQueryIterator{
	final Iterator<Binding> bindings;
	final PatternDocSet docset;
	
	public BindingsQueryIterator(final PatternDocSet docset, final List<Binding> bindings) {
		this.bindings = bindings.iterator();
		this.docset = docset;
	}
	
	@Override
	public void close() {
	}

	@Override
	public boolean hasNext() {
		return bindings.hasNext();
	}

	@Override
	public Binding next() {
		return bindings.next();
	}

	@Override
	public void remove() {
		bindings.remove();
	}

	@Override
	public void output(IndentedWriter out, SerializationContext sCxt) {
	}

	@Override
	public String toString(PrefixMapping pmap) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void output(IndentedWriter out) {
		// TODO Auto-generated method stub

	}

	@Override
	public Binding nextBinding() {
		return bindings.next();
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

	@Override
	public PatternDocSet patternDocSet() {
		return docset;
	}
}
