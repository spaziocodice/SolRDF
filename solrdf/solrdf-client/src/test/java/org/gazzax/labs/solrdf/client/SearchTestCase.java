package org.gazzax.labs.solrdf.client;

import static org.mockito.Mockito.mock;

import java.net.URI;

import org.apache.solr.client.solrj.SolrServer;
import org.junit.Before;
import org.junit.Ignore;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

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
		
	public static void main(String[] args) throws UnableToExecuteQueryException {
		try {
			SolRDF solrdf = SolRDF.newBuilder()
					.withEndpoint("http://127.0.0.1:8080/solr/store")
					.withGraphStoreProtocolEndpointPath("/rdf-graph-store")
					.withSPARQLEndpointPath("/sparql")
					.build();
			
			final ResultSet rs = solrdf.select("SELECT * FROM {?s ?p ?o}");
			ResultSetFormatter.asText(rs);
		} catch (final UnableToBuildSolRDFClientException exception) {
			
		}
	}
}