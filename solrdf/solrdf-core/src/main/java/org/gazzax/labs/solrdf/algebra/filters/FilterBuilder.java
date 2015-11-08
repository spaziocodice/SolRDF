package org.gazzax.labs.solrdf.algebra.filters;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class FilterBuilder {

	public String build(final Expr expression) {
		final StringBuilder builder = new StringBuilder();
		expression.visit(new ExprVisitor() {
			@Override
			public void visit(ExprAggregator eAgg) {
				// TODO Auto-generated method stub
			}
			
			@Override
			public void visit(final ExprVar var) {
				builder.append(var.getVarName());
			}
			
			@Override
			public void visit(final NodeValue value) {
				builder.append(value.asUnquotedString());
			}
			
			@Override
			public void visit(ExprFunctionOp funcOp) {
				// TODO Auto-generated method stub
			}
			
			@Override
			public void visit(ExprFunctionN func) {
				// TODO Auto-generated method stub
			}
			
			@Override
			public void visit(ExprFunction3 func) {
				// TODO Auto-generated method stub
			}
			
			@Override
			public void visit(final ExprFunction2 function) {
				final String name = function.getOpName();
				function.getArg1().visit(this);
				if ("=".equals(name)) {
					builder.append(":");
					function.getArg2().visit(this);
				} else if (">".equals(name)) {
					builder.append(":{");
					function.getArg2().visit(this);
					builder.append(" TO *]");
				} else if (">=".equals(name)) {
					builder.append(":[");
					function.getArg2().visit(this);
					builder.append(" TO *]");					
				} else if ("<".equals(name)) {
					builder.append(":[* TO ");
					function.getArg2().visit(this);
					builder.append("}");										
				} else if ("<=".equals(name)) {
					builder.append(":[* TO ");
					function.getArg2().visit(this);
					builder.append("]");										
				}
			}
			
			@Override
			public void visit(final ExprFunction1 function) {
			}
			
			@Override
			public void visit(ExprFunction0 func) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void startVisit() {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void finishVisit() {
				// TODO Auto-generated method stub
				
			}
		});
		return builder.toString();
	}
}
