package org.gazzax.labs.solrdf.handler.search.algebra.tt;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;

import static java.util.stream.Collectors.toList;

public class Main {
	public static void main(String[] args) {
		
		List<String> a = Arrays.asList("1","2","3","D","33");
		
		List<Integer> lenghts = a
				.stream()
				.filter(s -> s.length() == 1 && Character.isDigit(s.charAt(0)))
				.map(s -> Integer.parseInt(s))
				.collect(toList());
		
		System.out.println(lenghts);
		System.exit(0);
		Model m = ModelFactory.createDefaultModel();
		m.read(new StringReader(
					"<:a> <:first> \"Andrea\" . " +
					"<:b> <:first> \"Giorgio\" . " +
							
					"<:a> <:last> \"Gazzarini\" . " +
					"<:b> <:last> \"Gigioni\" . " +
					"<:b> <:last> \"Rossi\" . " +

					"<:a> <:tel> \"123\" . " +
					"<:b> <:tel> \"444\" . " +
					"<:b> <:tel> \"555\" . " +
					"<:b> <:tel> \"666\" . " +
					"")
				, "http://example.org","N-TRIPLES");
		
		QueryExecution ex = QueryExecutionFactory.create(
				"SELECT ?s ?first ?last ?tel "
				+ "WHERE "
				+ "{ "
				+ " 	?s <:first> ?first . "		
				+ " 	?s <:last> ?last . "
				+ " 	?s <:tel> ?tel . "
				+ " 	FILTER (?first > 20) "
				+ " 	FILTER (?last = \"Rossi\") "
				+ "} "
				, m);
		Op op = null;
		System.out.println(op =
//				Algebra.optimize(
						Algebra.compile(ex.getQuery()));
		
		OpFilter f = (OpFilter)((OpProject)op).getSubOp();
		for (Expr exp : f.getExprs()) {
			System.out.println(exp);
			System.out.println(exp.isConstant());
			System.out.println(exp.isFunction());
			System.out.println(exp.isVariable());
			System.out.println("----------------------------");
			ExprFunction func = exp.getFunction();
		}
		System.out.println(ResultSetFormatter.asText(ex.execSelect()));
		ex.close();
	}
}
