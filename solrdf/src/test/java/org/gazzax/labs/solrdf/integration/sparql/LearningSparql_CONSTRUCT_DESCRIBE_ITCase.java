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
 * CONSTRUCT Integration tests using examples taken from LearningSPARQL book.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see http://learningsparql.com
 */  
public class LearningSparql_CONSTRUCT_DESCRIBE_ITCase extends IntegrationTestSupertypeLayer {
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
	public void copyingData() throws Exception {
		constructTest(misteryGuest("ex176.rq", "ex012.ttl"));		
	}
	
	@Test
	public void copyingDataFromAnotherGraph() throws Exception {
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.1#"), "ex012.ttl"));		
		load(misteryGuest(null, NodeFactory.createURI("http://example.org.1#"), "ex125.ttl"));
		
		constructTest(misteryGuest("ex180.rq"));	
	}
	
	@Test
	public void creatingNewData_I() throws Exception {
		constructTest(misteryGuest("ex184.rq"));		
	}
	
	@Test
	public void creatingNewData_II() throws Exception {
		constructTest(misteryGuest("ex185.rq"));		
	}
	
	@Test
	public void creatingNewData_III() throws Exception {
		constructTest(misteryGuest("ex188.rq"));		
	}
	
	@Test
	public void creatingNewData_IV() throws Exception {
		constructTest(misteryGuest("ex190.rq"));		
	}
	
	@Test
	public void creatingNewData_V() throws Exception {
		constructTest(misteryGuest("ex192.rq"));		
	}
	
	@Test
	public void convertingData() throws Exception {
		constructTest(misteryGuest("ex194.rq", "ex012.ttl"));		
	}
	
	@Test
	@Ignore
	public void convertingDataFromRemoteService() throws Exception {
		constructTest(misteryGuest("ex196.rq"));		
	}

	@Test
	public void generatingDataAboutBrokenRules_I() throws Exception {
		constructTest(misteryGuest("ex203.rq", "ex198.ttl"));		
	}
	
	@Test
	public void generatingDataAboutBrokenRules_II() throws Exception {
		constructTest(misteryGuest("ex205.rq", "ex198.ttl"));		
	}
	
	@Test
	public void generatingDataAboutBrokenRules_III() throws Exception {
		constructTest(misteryGuest("ex207.rq", "ex198.ttl"));		
	}
	
	@Test
	public void generatingDataAboutBrokenRules_I_II_III() throws Exception {
		constructTest(misteryGuest("ex209.rq", "ex198.ttl"));		
	}
	
	@Test
	public void describeResource() throws Exception {
		describeTest(misteryGuest("ex213.rq", "ex069.ttl"));		
	}	
	
	@Test
	public void describeWithTriplePattern() throws Exception {
		describeTest(misteryGuest("ex216.rq", "ex069.ttl"));		
	}	
	
	@Test
	public void ifFunction() throws Exception {
		constructTest(misteryGuest("ex237.rq", "ex104.ttl"));		
	}	
	
	@Test
	@Ignore
	public void nodeTypeConversionFunction() throws Exception {
		constructTest(misteryGuest("ex246.rq", "ex241.ttl"));		
	}		
	
	@Override
	protected String examplesDirectory() {
		return LEARNING_SPARQL_EXAMPLES_DIR;
	}
}