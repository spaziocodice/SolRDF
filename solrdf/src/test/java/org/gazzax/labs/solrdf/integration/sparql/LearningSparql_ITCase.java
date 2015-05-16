/*
 * This Test case makes use of some examples from 
 * 
 * "Learning SPARQL - Querying and Updating with SPARQL 1.1" by Bob DuCharme
 * 
 * Publisher: O'Reilly
 * Author: Bob DuCharme
 * ISBN: 978-1-449-37143-2
 * http://www.learningsparql.com/
 * http://shop.oreilly.com/product/0636920030829.do
 * 
 * We warmly appreciate and thank the author and O'Reilly for such permission.
 * 
 */
package org.gazzax.labs.solrdf.integration.sparql;

import static org.gazzax.labs.solrdf.MisteryGuest.misteryGuest;

import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;

/**
 * Facet Object Queries integration test.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 */  
public class LearningSparql_ITCase extends IntegrationTestSupertypeLayer {
	protected final static String LEARNING_SPARQL_EXAMPLES_DIR = "src/test/resources/LearningSPARQLExamples";

	/** 
	 * Shutdown procedure for this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	@After
	public void tearDown() throws Exception {
		clearDatasets();
	}
	
	@Test
	public void queryWithPrefixes_I() throws Exception {
		execute(misteryGuest("ex003.rq", "ex002.ttl"));		
	}
	
	@Test
	public void queryWithPrefixes_II() throws Exception {
		execute(misteryGuest("ex006.rq", "ex002.ttl"));		
	}

	@Test
	public void fromKeyword() throws Exception {
		execute(misteryGuest("ex007.rq", "ex002.ttl"));		
	}

	@Test
	public void queryWithOneVariable() throws Exception {
		execute(misteryGuest("ex008.rq", "ex002.ttl"));		
	}

	@Test
	public void queryWithTwoVariables() throws Exception {
		execute(misteryGuest("ex010.rq", "ex002.ttl"));		
	}

	@Test
	public void multipleTriplePatterns_I() throws Exception {
		execute(misteryGuest("ex013.rq", "ex012.ttl"));		
	}

	@Test
	public void multipleTriplePatterns_II() throws Exception {
		execute(misteryGuest("ex015.rq", "ex012.ttl"));		
	}

	@Test
	public void humanReadableAnswersWithLabels() throws Exception {
		execute(misteryGuest("ex017.rq", "ex012.ttl"));		
	}

	@Test
	public void entityDescriptionQuery() throws Exception {
		execute(misteryGuest("ex019.rq", "ex012.ttl"));		
	}

	@Test
	public void filterWithRegularExpression() throws Exception {
		execute(misteryGuest("ex021.rq", "ex012.ttl"));		
	}

	@Test
	public void selectFirstAndLastName() throws Exception {
		execute(misteryGuest("ex047.rq", "ex012.ttl"));		
	}

	@Test
	public void queryingFOAFLabels() throws Exception {
		execute(misteryGuest("ex052.rq", "ex050.ttl", "foaf.rdf"));		
	}
	
	@Test
	public void dataThatMightBeNotThere() throws Exception {
		execute(misteryGuest("ex055.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalKeyword() throws Exception {
		execute(misteryGuest("ex057.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalGraphPattern() throws Exception {
		execute(misteryGuest("ex059.rq", "ex054.ttl"));		
	}	

	@Test
	public void optionalGraphPatterns() throws Exception {
		execute(misteryGuest("ex061.rq", "ex054.ttl"));		
	}	

	@Test
	public void orderOfOptionalGraphPatterns() throws Exception {
		execute(misteryGuest("ex063.rq", "ex054.ttl"));		
	}	

	@Test
	public void boundKeyword() throws Exception {
		execute(misteryGuest("ex065.rq", "ex054.ttl"));		
	}	

	@Test
	public void filterNotExists() throws Exception {
		execute(misteryGuest("ex067.rq", "ex054.ttl"));		
	}	

	@Test
	public void minusKeyword() throws Exception {
		execute(misteryGuest("ex068.rq", "ex054.ttl"));		
	}	

	@Test
	public void multipleTables() throws Exception {
		execute(misteryGuest("ex070.rq", "ex069.ttl"));		
	}	

	@Test
	public void multipleTablesWithSplitDatasets() throws Exception {
		execute(misteryGuest("ex070.rq", "ex072.ttl", "ex073.ttl", "ex368.ttl"));		
	}	

	@Test
	public void bindEither_I() throws Exception {
		execute(misteryGuest("ex075.rq", "ex074.ttl"));		
	}	
	
	@Test
	public void searchingWithBlankNodes() throws Exception {
		execute(misteryGuest("ex088.rq", "ex041.ttl"));		
	}		
	
	@Test
	public void duplicates() throws Exception {
		execute(misteryGuest("ex090.rq", "ex069.ttl"));		
	}		
	
	@Test
	public void distinct_I() throws Exception {
		execute(misteryGuest("ex092.rq", "ex069.ttl"));		
	}	
	
	@Test
	public void distinct_II() throws Exception {
		execute(misteryGuest("ex094.rq", "ex069.ttl"));		
	}	
	
	@Test
	public void union_I() throws Exception {
		execute(misteryGuest("ex101.rq", "ex100.ttl"));		
	}	
	
	@Test
	public void union_II() throws Exception {
		execute(misteryGuest("ex103.rq", "ex100.ttl"));		
	}	
	
	@Test
	public void filterWithSimpleMathExpression() throws Exception {
		execute(misteryGuest("ex105.rq", "ex104.ttl"));		
	}	

	@Test
	public void filterWithIsURIFunction() throws Exception {
		execute(misteryGuest("ex107.rq", "ex104.ttl"));		
	}	
	
	@Test
	public void filterWithINFunction() throws Exception {
		execute(misteryGuest("ex109.rq", "ex104.ttl"));		
	}	
	
	@Test
	public void filterWithNOTINFunction() throws Exception {
		execute(misteryGuest("ex112.rq", "ex104.ttl"));		
	}	
	
	@Test
	public void limit() throws Exception {
		execute(misteryGuest("ex116.rq", "ex115.ttl"));		
	}	
	
	@Test
	public void offset() throws Exception {
		execute(misteryGuest("ex118.rq", "ex115.ttl"));		
	}	

	@Test
	public void limitWithOffset() throws Exception {
		execute(misteryGuest("ex120.rq", "ex115.ttl"));		
	}	

	/**
	 * Curiously the Jena memory mode fails while SolRDF returns expected results
	 */
	@Test
	@Ignore
	public void from() throws Exception {
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.1#"), "ex069.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.2#"), "ex122.ttl"));
		
		execute(misteryGuest("ex123.rq"));		
	}	
	
	@Test
	@Ignore
	public void fromNamed() throws Exception {
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.1#"), "ex069.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.2#"), "ex122.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.3#"), "ex125.ttl"));
		
		execute(misteryGuest("ex126.rq"));		
	}	
	
	@Test
	public void subQueries() throws Exception {
		execute(misteryGuest("ex137.rq", "ex069.ttl"));		
	}	
	
	@Test
	public void combiningValuesAndAssigningValuesToVariables_I() throws Exception {
		execute(misteryGuest("ex139.rq", "ex138.ttl"));		
	}		
	
	@Test
	public void combiningValuesAndAssigningValuesToVariables_II() throws Exception {
		execute(misteryGuest("ex141.rq", "ex138.ttl"));		
	}		
	
	@Test
	public void combiningValuesAndAssigningValuesToVariables_withSubqueries() throws Exception {
		execute(misteryGuest("ex143.rq", "ex138.ttl"));		
	}			
	
	@Test
	public void bindKeyword() throws Exception {
		execute(misteryGuest("ex144.rq", "ex138.ttl"));		
	}	
	
	@Test
	public void valueKeyword_I() throws Exception {
		execute(misteryGuest("ex492.rq", "ex138.ttl"));		
	}	
	
	@Test
	public void valueKeyword_II() throws Exception {
		execute(misteryGuest("ex496.rq", "ex145.ttl"));		
	}		
	
	@Test
	public void valueKeyword_III() throws Exception {
		execute(misteryGuest("ex498.rq", "ex145.ttl"));		
	}		
	
	@Test
	public void valueKeyword_IV() throws Exception {
		execute(misteryGuest("ex500.rq", "ex145.ttl"));		
	}		

	@Test
	public void orderByKeywordAsc() throws Exception {
		execute(misteryGuest("ex146.rq", "ex145.ttl"));		
	}		
	
	@Test
	public void orderByKeywordDesc() throws Exception {
		execute(misteryGuest("ex148.rq", "ex145.ttl"));		
	}		
	
	@Test
	public void orderByKeywordWithTwoKeys() throws Exception {
		execute(misteryGuest("ex149.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void limitAndOrderKeywords() throws Exception {
		execute(misteryGuest("ex151.rq", "ex145.ttl"));		
	}		
	
	@Test
	public void maxFunction_I() throws Exception {
		execute(misteryGuest("ex153.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void maxFunction_II() throws Exception {
		execute(misteryGuest("ex155.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void avgFunction() throws Exception {
		execute(misteryGuest("ex156.rq", "ex145.ttl"));		
	}	
	
	/**
	 * Fails because of the different order of the results.
	 */
	@Test
	@Ignore
	public void groupConcatFunction() throws Exception {
		execute(misteryGuest("ex158.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void groupByKeyword() throws Exception {
		execute(misteryGuest("ex160.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void countFunction() throws Exception {
		execute(misteryGuest("ex162.rq", "ex145.ttl"));		
	}	
	
	@Test
	public void havingKeyword() throws Exception {
		execute(misteryGuest("ex164.rq", "ex145.ttl"));		
	}
	
	@Test
	@Ignore
	public void serviceKeyword() throws Exception {
		execute(misteryGuest("ex167.rq", "ex145.ttl"));		
	}
	
	@Override
	protected String examplesDirectory() {
		return LEARNING_SPARQL_EXAMPLES_DIR;
	}
}