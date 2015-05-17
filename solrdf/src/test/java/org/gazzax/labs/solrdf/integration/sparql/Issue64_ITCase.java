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
 * Querying string literals with diacritics or angle brackets yields no result.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see 
 */  
public class Issue64_ITCase extends IntegrationTestSupertypeLayer {
	protected final static String EXAMPLES_DIR = "src/test/resources/sample_data";
	
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
	public void shouldReturnOneBinding() throws Exception {
		selectTest(misteryGuest("issue_64_wrong_results.rq", "issue_64_wrong_results.ttl"));		
	}

	@Override
	protected String examplesDirectory() {
		return EXAMPLES_DIR;
	}	
}