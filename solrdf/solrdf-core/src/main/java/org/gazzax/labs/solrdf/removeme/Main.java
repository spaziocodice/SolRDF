package org.gazzax.labs.solrdf.removeme;

import static org.gazzax.labs.solrdf.NTriples.asNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.Field;
import org.gazzax.labs.solrdf.handler.search.algebra.CompositePatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.LeafPatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.PatternDocSet;
import org.gazzax.labs.solrdf.handler.search.algebra.tt.ExtendedQueryIterator;
import org.gazzax.labs.solrdf.handler.search.handler.BindingsQueryIterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterFilterExpr;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.engine.main.OpExecutorFactory;
import com.hp.hpl.jena.sparql.engine.main.QC;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;

public class Main {
	public static void main(String[] args) {
		Model m = ModelFactory.createDefaultModel();
		
		String a  = 
				"PREFIX ab: <http://learningsparql.com/ns/addressbook#> \n"+ 
				"SELECT * \n"+
				"WHERE \n"+
				"{ \n"+
				"  ?s ab:firstName ?first ; \n"+
				"     ab:lastName ?last ; \n"+
				"     ab:portfolio ?amount . \n"+
				"  FILTER (?amount > 10000) \n" +				
				"  OPTIONAL  \n"+
				"  { ?s ab:workTel ?workTel . } \n"+
				"}";
		
		Query q = QueryFactory.create(a);
		System.out.println(
				Algebra.optimize(
						Algebra.compile(q)));
		
		final OpVisitor v = new MyOpVisitor();
		
		// FIXME : Tutta questa roba non deve stare qua (semmai sia utile)
		OpExecutorFactory factory = new OpExecutorFactory() {
			
			@Override
			public OpExecutor create(ExecutionContext execCxt) {
				return new OpExecutor(execCxt) {
					
					@Override
					protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
						return super.execute(opCondition, input);
					}
					
					protected QueryIterator execute(OpSequence opSequence, QueryIterator input) {
						return super.execute(opSequence, input);
					};
					
					@Override
					protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
//						Reducible reducible = getReducer(opFilter.getSubOp());
//						reducible.accept(opFilter);
//						
//						reducible
						return super.execute(opFilter, input);
					}
					
					
					@Override
					protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
						System.out
								.println("Main.main(...).new OpExecutorFactory() {...}.create(...).new OpExecutor() {...}.execute()");
//						BgpReducer reducer = new BgpReducer(opBGP.getPattern(), execCxt);
//						reducer.reduce();
						return super.exec(opBGP, input);
					}
				};
			}
		};
		QC.setFactory(ARQ.getContext(), factory);
		
		QueryExecution ex = QueryExecutionFactory.create(q, m);
		System.out.println(ResultSetFormatter.asText(ex.execSelect()));
	}
}