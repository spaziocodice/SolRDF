package org.gazzax.labs.solrdf.algebra.filters;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDecimal;

public class FilterBuilderTestCase {
	
	private final String amount = "amount";
	private final BigDecimal $20_3 = BigDecimal.valueOf(20.3);
	private FilterBuilder builder;
	
	@Before
	public void setUp() {
		builder = new FilterBuilder();
	}

	@Test
	public void lessThan() {
		assertEquals(
				"amount:[* TO 20.3}", 
				builder.build(
						new E_LessThan(
								new ExprVar(NodeFactory.createVariable(amount)),
								new NodeValueDecimal($20_3))));
	}

	@Test
	public void greaterThan() {
		assertEquals(
				"amount:{20.3 TO *]", 
				builder.build(
						new E_GreaterThan(
								new ExprVar(NodeFactory.createVariable(amount)),
								new NodeValueDecimal($20_3))));
	}
	
	@Test
	public void equals() {
		assertEquals(
				"amount:20.3", 
				builder.build(
						new E_Equals(
								new ExprVar(NodeFactory.createVariable(amount)),
								new NodeValueDecimal($20_3))));
	}
	
	@Test
	public void greaterThanOrEquals() {
		assertEquals(
				"amount:[20.3 TO *]", 
				builder.build(
						new E_GreaterThanOrEqual(
								new ExprVar(NodeFactory.createVariable(amount)),
								new NodeValueDecimal($20_3))));
	}	
	
	@Test
	public void lessThanOrEquals() {
		assertEquals(
				"amount:[* TO 20.3]", 
				builder.build(
						new E_LessThanOrEqual(
								new ExprVar(NodeFactory.createVariable(amount)),
								new NodeValueDecimal($20_3))));
	}		
}