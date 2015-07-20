package org.gazzax.labs.solrdf.client;

import static org.mockito.Mockito.mock;

import java.io.FileReader;
import java.net.URI;

import org.apache.solr.client.solrj.SolrServer;
import org.gazzax.labs.solrdf.client.SolRDF.CloseableResultSet;
import org.junit.Before;
import org.junit.Ignore;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Test case which elencates several ways for adding data to SolRDF.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
@Ignore
public class SearchTestCase {
	String uri;
	DatasetAccessor dataset;
	SolrServer solr;
	SolRDF solrdf;

	/**
	 * Setup fixture for this test case.
	 * 
	 * @throws Exception never, otherwise the corresponding test fails.
	 */
	@Before
	public void setUp() throws Exception{
		dataset = mock(DatasetAccessor.class);
		solr = mock(SolrServer.class);
		solrdf = new SolRDF(dataset, "/sparql", solr);
		uri = new URI("http://org.example.blablabla").toString();
	}
		
	public static void main(String[] args) throws Exception {
		try {
			SolRDF solrdf = SolRDF.newBuilder()
					.withEndpoint("http://127.0.0.1:8080/solr/store")
					.withGraphStoreProtocolEndpointPath("/rdf-graph-store")
					.withSPARQLEndpointPath("/sparql")
					.build();
			
			solrdf.add(new FileReader("/work/workspaces/solrdf/solrdf/solrdf/solrdf-integration-tests/src/test/resources/sample_data/faceting_test_dataset.nt"), "N-TRIPLES");
			
			solrdf.commit();
			
			CloseableResultSet rs = null;
			try {
				rs = solrdf.select("SELECT * WHERE {?s ?p ?o}");
				System.out.println(ResultSetFormatter.asText(rs));
				
				Model m = solrdf.construct("DESCRIBE <http://example.org/book4>");
				System.out.println(m);
			} finally {
				rs.close();
			}
		} catch (final UnableToBuildSolRDFClientException exception) {
			exception.printStackTrace();
		}
	}
}