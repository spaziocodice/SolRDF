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
import org.junit.Test;

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
	public void regularExpression() throws Exception {
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
	
	@Override
	protected String examplesDirectory() {
		return LEARNING_SPARQL_EXAMPLES_DIR;
	}
}