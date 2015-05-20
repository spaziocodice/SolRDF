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

import org.gazzax.labs.solrdf.MisteryGuest;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * SPARQL Update Integration tests using examples taken from LearningSPARQL book.
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see http://learningsparql.com
 */  
public class LearningSparql_UPDATE_ITCase extends LearningSparqlSupertypeLayer {
	
	/**
	 * Executes a given update command both on remote and local model.
	 * 
	 * @param data the object holding test data (i.e. commands, queries, datafiles).
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	void executeUpdate(final MisteryGuest data) throws Exception {
		final String updateCommandString = readFile(data.query);
		UpdateExecutionFactory.createRemote(UpdateFactory.create(updateCommandString), SPARQL_ENDPOINT_URI).execute();

		commitChanges();

		final Model memoryModel = memoryDataset.getDefaultModel();
		UpdateAction.parseExecute(updateCommandString, memoryModel);
		
		assertIsomorphic(memoryModel, DATASET.getModel());
	}
	
	@Test
	public void insertDataKeyword() throws Exception {
		executeUpdate(misteryGuest("ex312.ru"));
	}
	
	@Test
	public void insertKeyword() throws Exception {
		load(misteryGuest("", "ex012.ttl"));
		executeUpdate(misteryGuest("ex313.ru"));
	}	
	
	@Test
	public void insertAsConstructThatChangesData() throws Exception {
		load(misteryGuest("", "ex012.ttl"));
		executeUpdate(misteryGuest("ex316.ru"));
	}
}