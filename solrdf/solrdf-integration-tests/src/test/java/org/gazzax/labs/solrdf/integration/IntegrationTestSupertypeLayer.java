package org.gazzax.labs.solrdf.integration;

import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.SchemePortResolver;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.gazzax.labs.solrdf.MisteryGuest;
import org.gazzax.labs.solrdf.client.SolRDF;
import org.gazzax.labs.solrdf.client.UnableToBuildSolRDFClientException;
import org.gazzax.labs.solrdf.log.Log;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;

/**
 * Supertype layer for all integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class IntegrationTestSupertypeLayer extends SolrJettyTestBase {
	protected static final String SOLR_URI = "http://127.0.0.1:8080/solr/store";
	protected static final String SPARQL_ENDPOINT_URI = SOLR_URI + "/sparql";
	protected static final String GRAPH_STORE_ENDPOINT_URI = SOLR_URI + "/rdf-graph-store";
	
	protected final Log log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected static HttpSolrClient PLAIN_SOLR_CLIENT;
	protected QueryExecution inMemoryExecution;
	protected Dataset memoryDataset;
	
	protected static SolRDF SOLRDF_CLIENT;
	protected static JettySolrRunner SOLR;
	
	/**
	 * Initilisation procedure for this test case.
	 * 
	 * @throws UnableToBuildSolRDFClientException in case the client cannot be built.
	 * @throws Exception in case of Solr startup failure.
	 */
	@BeforeClass
	public static void initITTest() {
		System.setProperty("tests.asserts", "false");	
		System.setProperty("jetty.port", "8080");
		System.setProperty("solr.core.name", "store");
		System.setProperty("solr.data.dir", initCoreDataDir.getAbsolutePath());
			
		try {
			SOLR = createJetty(
					"target/solrdf-integration-tests-1.1-dev/solrdf",
					JettyConfig.builder()
						.setPort(8080)
						.setContext("/solr")
						.stopAtShutdown(true)
						.build());		
			
			final HttpClient httpClient = HttpClientBuilder.create()
					.setRoutePlanner(
							new DefaultRoutePlanner(
									new SchemePortResolver() {
										@Override
										public int resolve(final HttpHost host) throws UnsupportedSchemeException {
											return SOLR.getLocalPort();
										}
									})).build();
			
			
			SOLRDF_CLIENT = SolRDF.newBuilder()
		              .withEndpoint("http://127.0.0.1:8080/solr/store")
		              .withGraphStoreProtocolEndpointPath("/rdf-graph-store")
		              .withHttpClient(httpClient)
		              .withSPARQLEndpointPath("/sparql")
		              .build();
			
			PLAIN_SOLR_CLIENT = new HttpSolrClient(SOLR_URI);
		} catch (final Exception exception) {
			throw new RuntimeException(exception);
		}
	}
	
	/**
	 * Shutdown procedure for this test case.
	 * 
	 * @throws Exception hopefully never.
	 */
	@AfterClass
	public static void shutdown() throws Exception {
		clearData();
		SOLRDF_CLIENT.done();
		PLAIN_SOLR_CLIENT.close();
	}
	
	/**
	 * Setup fixture for this test.
	 */
	@Before
	public void setUp() throws Exception {
		super.setUp();
		memoryDataset = DatasetFactory.createMem();
	}
	 
	/** 
	 * Shutdown procedure for this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	@After
	public void tearDown() throws Exception {
		super.tearDown();
		clearDatasets();
		if (inMemoryExecution != null) {
			inMemoryExecution.close();
		}
	}		
	
	/**
	 * Removes all data created by this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	protected void clearDatasets() throws Exception {
		clearData();
		if (memoryDataset != null) {
			for (final Iterator<String> graphsIterator = memoryDataset.listNames(); graphsIterator.hasNext();)
			{
				memoryDataset.getNamedModel(graphsIterator.next()).removeAll();
			}
			memoryDataset.getDefaultModel().removeAll();
		}
	}
	
	/**
	 * Cleans all data previously indexed on SolRDF.
	 * 
	 * @throws Exception hopefully never.
	 */
	protected static void clearData() throws Exception {
		SOLRDF_CLIENT.clear();
		SOLRDF_CLIENT.commit();
	}
	
	/**
	 * Reads a query from the file associated with this test and builds a query string.
	 * 
	 * @param filename the filename.
	 * @return the query string associated with this test.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String queryString(final String filename) throws IOException {
		return readFile(filename);
	}
	
	/**
	 * Builds a string from a given file.
	 * 
	 * @param filename the filename (without path).
	 * @return a string with the file content.
	 * @throws IOException in case of I/O failure while reading the file.
	 */
	protected String readFile(final String filename) throws IOException {
		return new String(Files.readAllBytes(Paths.get(source(filename))));
	}	
	
	/**
	 * Returns the URI of a given filename.
	 * 
	 * @param filename the filename.
	 * @return the URI (as string) of a given filename.
	 */ 
	protected URI source(final String filename) {
		return new File(examplesDirectory(), filename).toURI();
	}	

	/**
	 * Executes a given ASK query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void askTest(final MisteryGuest data) throws Exception {
		load(data);
		
		inMemoryExecution = QueryExecutionFactory.create(QueryFactory.create(queryString(data.query)), memoryDataset);
			
		assertEquals(
				Arrays.toString(data.datasets) + ", " + data.query,
				inMemoryExecution.execAsk(),
				SOLRDF_CLIENT.ask(queryString(data.query)));
	}
		
	/**
	 * Executes a given CONSTRUCT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void describeTest(final MisteryGuest data) throws Exception {
		load(data);
		
		final Query query = QueryFactory.create(queryString(data.query));
		try {
			inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset);
			
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					inMemoryExecution.execDescribe().isIsomorphicWith(
							SOLRDF_CLIENT.describe(queryString(data.query))));
		} catch (final Throwable error) {
			StringWriter writer = new StringWriter();
			RDFDataMgr.write(writer, SOLRDF_CLIENT.describe(queryString(data.query)), RDFFormat.NTRIPLES);
			log.debug("JNS\n" + writer);
			
			QueryExecution debugExecution = QueryExecutionFactory.create(query, memoryDataset);
			writer = new StringWriter();
			RDFDataMgr.write(writer, debugExecution.execDescribe(), RDFFormat.NTRIPLES);
			
			log.debug("MEM\n" + writer);
			
			debugExecution.close();
			throw error;
		} 
	}	
	
	/**
	 * Executes a given CONSTRUCT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void constructTest(final MisteryGuest data) throws Exception {
		load(data);
		
		try {
			inMemoryExecution = QueryExecutionFactory.create(
					QueryFactory.create(queryString(data.query)), memoryDataset);
			
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					inMemoryExecution.execConstruct().isIsomorphicWith(
							SOLRDF_CLIENT.construct(queryString(data.query))));
		} catch (final Throwable error) {
			StringWriter writer = new StringWriter();
			RDFDataMgr.write(writer, SOLRDF_CLIENT.construct(queryString(data.query)), RDFFormat.NTRIPLES);
			log.debug("JNS\n" + writer);
			
			QueryExecution debugExecution = QueryExecutionFactory.create(
					QueryFactory.create(queryString(data.query)), memoryDataset);
			
			writer = new StringWriter();
			RDFDataMgr.write(writer, debugExecution.execConstruct(), RDFFormat.NTRIPLES);
			
			log.debug("MEM\n" + writer);
			
			debugExecution.close();
			throw error;
		} 
	}
	
	/**
	 * Executes a given SELECT query against a given dataset.
	 * 
	 * @param data the mistery guest containing test data (query and dataset)
	 * @throws Exception never, otherwise the test fails.
	 */
	protected void selectTest(final MisteryGuest data) throws Exception {
		load(data);
		
		try {
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					ResultSetCompare.isomorphic(
							SOLRDF_CLIENT.select(queryString(data.query)),
							(inMemoryExecution = QueryExecutionFactory.create(
									QueryFactory.create(queryString(data.query)), 
									memoryDataset)).execSelect()));
		} catch (final Throwable error) {
			log.debug("JNS\n" + ResultSetFormatter.asText(SOLRDF_CLIENT.select(queryString(data.query))));
			
			QueryExecution debugExecution = null;
			log.debug("MEM\n" + ResultSetFormatter.asText(
					(debugExecution = (QueryExecutionFactory.create(
							QueryFactory.create(queryString(data.query)), 
							memoryDataset))).execSelect()));
			
			debugExecution.close();
			throw error;
		} 
	}
	
	/**
	 * Executes a given update command both on remote and local model.
	 * 
	 * @param data the object holding test data (i.e. commands, queries, datafiles).
	 * @throws Exception hopefully never otherwise the corresponding test fails.
	 */
	protected void executeUpdate(final MisteryGuest data) throws Exception {
		load(data);
		
		final String updateCommandString = readFile(data.query);
		UpdateExecutionFactory.createRemote(UpdateFactory.create(updateCommandString), SPARQL_ENDPOINT_URI).execute();

		SOLRDF_CLIENT.commit();

		UpdateAction.parseExecute(updateCommandString, memoryDataset.asDatasetGraph());
		
		final Iterator<Node> nodes = memoryDataset.asDatasetGraph().listGraphNodes();
		if (nodes != null) {
			while (nodes.hasNext()) {
				final Node graphNode = nodes.next();
				final String graphUri = graphNode.getURI();
				final Model inMemoryNamedModel = memoryDataset.getNamedModel(graphUri);
				assertIsomorphic(inMemoryNamedModel, SOLRDF_CLIENT.getNamedModel(graphUri), graphUri);		
			}
		}
		
		assertIsomorphic(memoryDataset.getDefaultModel(), SOLRDF_CLIENT.getDefaultModel(), null);			
	}
	
	/**
	 * Loads all triples found in the datafile associated with the given name.
	 * 
	 * @param datafileName the name of the datafile.
	 * @param graphs an optional set of target graph URIs. 
	 * @throws Exception hopefully never, otherwise the test fails.
	 */
	protected void load(final MisteryGuest data) throws Exception {
		if (data.datasets == null || data.datasets.length == 0) {
			return;
		}
		
		final Model memoryModel = data.graphURI != null 
				? memoryDataset.getNamedModel(data.graphURI) 
				: memoryDataset.getDefaultModel();
				 
		for (final String datafileName : data.datasets) {
			final String dataURL = source(datafileName).toString();
			final String lang = datafileName.endsWith("ttl") 
					? "TTL" 
					: datafileName.endsWith("nt") 
						? "N-TRIPLES" 
						: null;
			memoryModel.read(dataURL, DUMMY_BASE_URI, lang);
		}  
  
		if (data.graphURI != null) {
			SOLRDF_CLIENT.add(data.graphURI, memoryModel.listStatements());
		} else {
			SOLRDF_CLIENT.add(memoryModel.listStatements());
		}
		
		SOLRDF_CLIENT.commit();
		
		final Iterator<Node> nodes = memoryDataset.asDatasetGraph().listGraphNodes();
		if (nodes != null) {
			while (nodes.hasNext()) {
				final Node graphNode = nodes.next();
				final String graphUri = graphNode.getURI();
				final Model inMemoryNamedModel = memoryDataset.getNamedModel(graphUri);
				assertIsomorphic(inMemoryNamedModel, SOLRDF_CLIENT.getNamedModel(graphUri), graphUri);		
			}
		}
		
		final Model model = (data.graphURI != null) ? SOLRDF_CLIENT.getNamedModel(data.graphURI) : SOLRDF_CLIENT.getDefaultModel();
		assertFalse(Arrays.toString(data.datasets) + ", " + data.query, model.isEmpty());
		assertIsomorphic(memoryModel, model, null);
	} 
	
	protected abstract String examplesDirectory();
	
	protected void assertIsomorphic(final Model memoryModel, final Model solrdfModel, final String uri) {
		try {
			assertTrue(solrdfModel.isIsomorphicWith(memoryModel));
		} catch (Throwable exception) {
			final StringWriter memoryModelWriter = new StringWriter();
			final StringWriter remoteModelWriter = new StringWriter();
			RDFDataMgr.write(memoryModelWriter, memoryModel, RDFFormat.NTRIPLES) ;
			RDFDataMgr.write(remoteModelWriter, solrdfModel, RDFFormat.NQUADS) ;

			final String name = uri != null ? uri : " (DEFAULT) ";
			log.debug("**** MEMORY MODEL " + name + " ****");
			log.debug(memoryModelWriter.toString());
			log.debug("");
			log.debug("**** REMOTE MODEL " + name + " ****");
			log.debug(remoteModelWriter.toString());
			log.debug("*********************************");
			throw exception;
		}
	}

}