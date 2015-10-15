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
import org.junit.Test;

/**
 * Querying string literals with diacritics or angle brackets yields no result.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see 
 */  
public class Issue112_ITCase extends IntegrationTestSupertypeLayer {
	@Test
	public void deleteEverything() throws Exception {
		executeUpdate(misteryGuest("issue_112_delete_all.ru", "faceting_test_dataset.nt"));	
	}

	@Test
	public void deleteEverythingAboutAResource() throws Exception {
		executeUpdate(misteryGuest("issue_112_delete_resource.ru", "faceting_test_dataset.nt"));	
	}

	@Test
	public void deleteEverythingWithDateObject() throws Exception {
		executeUpdate(misteryGuest("issue_112_delete_resources_with_a_date_object.ru", "faceting_test_dataset.nt"));	
	}
	
	@Test
	public void deleteWithDateSelector() throws Exception {
		executeUpdate(misteryGuest("issue_112_delete_with_a_date_selector.ru", "faceting_test_dataset.nt"));	
	}
	
	@Test
	public void insertDateTimeAndDeleteEverything() throws Exception {
		executeUpdate(misteryGuest("issue_112_insert_with_datetime.ru"));
		executeUpdate(misteryGuest("issue_112_delete_all.ru"));
	}
	
	@Override
	protected String examplesDirectory() {
		return "src/test/resources/sample_data";
	}	
}