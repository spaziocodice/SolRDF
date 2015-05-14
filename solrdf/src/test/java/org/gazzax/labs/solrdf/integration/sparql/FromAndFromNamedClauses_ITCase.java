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

import java.io.FileReader;

import org.gazzax.labs.solrdf.integration.IntegrationTestSupertypeLayer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Asserts what stated at the following page: http://answers.semanticweb.com/questions/11509/what-is-the-difference-between-from-and-from-named
 *  
 * @author Andrea Gazzarini
 * @since 1.0
 * @see http://answers.semanticweb.com/questions/11509/what-is-the-difference-between-from-and-from-named
 */  
@Ignore
public class FromAndFromNamedClauses_ITCase extends IntegrationTestSupertypeLayer {
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
	
	/**
	 * Loads data. 
	 * 
	 * @throws Exception hopefully never.
	 */
	@Before
	public void loadData() throws Exception {
		load(misteryGuest(null, NodeFactory.createURI("http://grapha.com"), "one_triple_1.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://graphb.com"), "one_triple_2.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://graphc.com"), "one_triple_3.ttl"));
		load(misteryGuest(null, NodeFactory.createURI("http://graphd.com"), "one_triple_4.ttl"));
	}
	
	/**
	 * SELECT ?s WHERE { ?s <p> ?o }
	 * will often give <a1>, <b1> <c1>, <d1>, but this depends on what the default graph is implicitly defined as.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void withoutAnyClause() throws Exception {
		execute(misteryGuest("answers_sw_1.rq"));		
	}
	
	public static void main(String[] args) throws Exception {
		Dataset memoryDataset = DatasetFactory.createMem();
		Model memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(new FileReader("/work/workspaces/rdf/SolRDF/solrdf/src/test/resources/sample_data/one_triple_1.ttl"), "http://e.org", "TTL");
		memoryDataset.addNamedModel("http://grapha.com", memoryModel);
		
		memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(new FileReader("/work/workspaces/rdf/SolRDF/solrdf/src/test/resources/sample_data/one_triple_2.ttl"), "http://e.org", "TTL");
		memoryDataset.addNamedModel("http://graphb.com", memoryModel);
		
		memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(new FileReader("/work/workspaces/rdf/SolRDF/solrdf/src/test/resources/sample_data/one_triple_3.ttl"), "http://e.org", "TTL");
		memoryDataset.addNamedModel("http://graphc.com", memoryModel);
		
		memoryModel = ModelFactory.createDefaultModel();
		memoryModel.read(new FileReader("/work/workspaces/rdf/SolRDF/solrdf/src/test/resources/sample_data/one_triple_4.ttl"), "http://e.org", "TTL");
		memoryDataset.addNamedModel("http://graphd.com", memoryModel);
		
		final Query query = QueryFactory.create(q2());//"SELECT ?s FROM <http://grapha.com> WHERE { ?s <http://example.org/title> ?o }");
		
		System.out.println(ResultSetFormatter.asText(QueryExecutionFactory.create(query, memoryDataset).execSelect()));
	}
	
	static String q1() {
		return "select ?s { graph <http://grapha.com> {  ?s ?p ?o } }";
	}
	
	static String q2() {
		return "select ?s from <http://grapha.com> { ?s ?p ?o }";
	}
	
	/**
	 * FROM <http://grapha.com>
	 * 
	 * SELECT ?s WHERE { ?s <p> ?o }
	 * 
	 * should give <a1>.
	 * 
	 * @throws Exception hopefully never otherwise the test fails.
	 */
	@Test
	public void singleFromClause() throws Exception {
		execute(misteryGuest("answers_sw_2.rq"));		
	}	

	@Override
	protected String examplesDirectory() {
		return EXAMPLES_DIR;
	}	
}