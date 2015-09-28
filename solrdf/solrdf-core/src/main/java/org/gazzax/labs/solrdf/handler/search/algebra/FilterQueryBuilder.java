package org.gazzax.labs.solrdf.handler.search.algebra;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.WildcardQuery;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.ExprVisitorBase;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class FilterQueryBuilder extends ExprVisitorBase {
	final Triple triple;
	final BooleanQuery query;
	
	private ExprVisitor langMatchesAdapter = new ExprVisitorBase() {
		private String field;
		private String value;

		@Override
		public void visit(final ExprFunction2 func) {
			func.getArg1().visit(this);
			func.getArg2().visit(this);
		}
		
		@Override
		public void visit(ExprFunction1 func) {
			func.getArg().visit(this);
		}
		
		public void visit(ExprVar nv) {
			if (triple.objectMatches(nv.getAsNode())) {
				this.field = "o_lang";				
			}
		};
		
		public void visit(final NodeValue nv) {
			if (field != null) {
				query.add(new WildcardQuery(new Term(field, nv.asUnquotedString() + "*")), Occur.MUST);
			}
		};
	};
	
	public FilterQueryBuilder(
			final Triple triple,
			final BooleanQuery query) {
		this.triple = triple;
		this.query = query;
	}
	
	@Override
	public void startVisit() {
		
	}

	@Override
	public void visit(ExprFunction0 func) {

	}

	@Override
	public void visit(ExprFunction1 func) {
	
	}

	@Override
	public void visit(final ExprFunction2 func) {
		if (func instanceof E_LangMatches) {
			func.visit(langMatchesAdapter);
		} else if (func instanceof E_LogicalOr) {
			
		}
	}

	@Override
	public void visit(ExprFunction3 func) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExprFunctionN func) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExprFunctionOp funcOp) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(NodeValue nv) {
		

	}

	@Override
	public void visit(ExprVar nv) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExprAggregator eAgg) {
		// TODO Auto-generated method stub

	}

	@Override
	public void finishVisit() {
		// TODO Auto-generated method stub

	}

}
