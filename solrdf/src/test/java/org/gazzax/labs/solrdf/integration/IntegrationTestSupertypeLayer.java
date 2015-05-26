package org.gazzax.labs.solrdf.integration;

import static org.gazzax.labs.solrdf.TestUtility.DUMMY_BASE_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.gazzax.labs.solrdf.MisteryGuest;
import org.gazzax.labs.solrdf.log.Log;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.resultset.ResultSetCompare;

/**
 * Supertype layer for all integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class IntegrationTestSupertypeLayer {
	protected static final String SOLR_URI = "http://127.0.0.1:8080/solr/store";
	protected static final String SPARQL_ENDPOINT_URI = SOLR_URI + "/sparql";
	protected static final String GRAPH_STORE_ENDPOINT_URI = SOLR_URI + "/rdf-graph-store";
	
	protected final Log log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected static HttpSolrServer solr;
	protected QueryExecution execution;
	protected QueryExecution inMemoryExecution;
	protected Dataset memoryDataset;
	protected static DatasetAccessor DATASET;
	
	/**
	 * Initilisation procedure for this test case.
	 * 
	 * @throws Exception hopefully never.
	 */
	@BeforeClass
	public static void initClient() {
		solr = new HttpSolrServer(SOLR_URI);
		resetSolRDFXmlResponseParser();
		DATASET = DatasetAccessorFactory.createHTTP(GRAPH_STORE_ENDPOINT_URI);
	}
	
	/**
	 * Shutdown procedure for this test case.
	 * 
	 * @throws Exception hopefully never.
	 */
	@AfterClass
	public static void shutdownClient() throws Exception {
		clearData();
		solr.shutdown();		
	}
	
	/**
	 * Setup fixture for this test.
	 */
	@Before
	public void setUp() throws Exception {
		memoryDataset = DatasetFactory.createMem();
	}
	 
	/** 
	 * Shutdown procedure for this test.
	 * 
	 * @throws Exception hopefully never.
	 */
	@After
	public void tearDown() throws Exception {
		if (execution != null) {
			execution.close();
		}
		
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
			memoryDataset.getDefaultModel().removeAll();
		}
	}
	
	/**
	 * Cleans all data previously indexed on SolRDF.
	 * 
	 * @throws Exception hopefully never.
	 */
	protected static void clearData() throws Exception {
		solr.setParser(new XMLResponseParser());
		solr.deleteByQuery("*:*");
		commitChanges();
		resetSolRDFXmlResponseParser();	}
	
	/**
	 * Commits changes on Solr.
	 * 
	 * @throws SolrServerException in case of a Solr failure.
	 * @throws IOException in case of I/O failure.
	 */
	protected static void commitChanges() throws SolrServerException, IOException {
		solr.setParser(new XMLResponseParser());
		solr.commit();		
		resetSolRDFXmlResponseParser();
	}	
	
	protected static void resetSolRDFXmlResponseParser() {
		solr.setParser(new XMLResponseParser() {
			  @Override
			  public String getContentType() {
			    return "text/xml";
			  }
			  
			  @Override
			  protected SolrDocumentList readDocuments(final XMLStreamReader parser) throws XMLStreamException {
				  return new SolrDocumentList();
			  }
			  
			  protected NamedList<Object> readNamedList(final XMLStreamReader parser) throws XMLStreamException {
				  if( XMLStreamConstants.START_ELEMENT != parser.getEventType()) {
					  throw new RuntimeException("must be start element, not: " + parser.getEventType());
				  }

				  final StringBuilder builder = new StringBuilder();
				  final NamedList<Object> nl = new SimpleOrderedMap<>();
				  KnownType type = null;
				  String name = null;
			    
				  int depth = 0;
				  while( true ) {
					  switch (parser.next()) {
					  case XMLStreamConstants.START_ELEMENT:
						  depth++;
						  builder.setLength( 0 ); 
						  type = KnownType.get( parser.getLocalName() );
						  if( type == null ) {
							  continue;
						  }
						  
						  name = null;
						  int cnt = parser.getAttributeCount();
						  for( int i=0; i<cnt; i++ ) {
							  if( "name".equals( parser.getAttributeLocalName( i ) ) ) {
								  name = parser.getAttributeValue( i );
								  break;
							  }
						  }
						  if (type == KnownType.LST) {
							  nl.add( name, readNamedList( parser ) ); depth--; continue;
						  } else if (type == KnownType.ARR) {
							  nl.add( name, readArray(     parser ) ); depth--; continue;
						  }
						  break;
					  case XMLStreamConstants.END_ELEMENT:
						  if( --depth < 0 ) {
							  return nl;
						  }
						  
						  if (type != null) {
							  nl.add( name, type.read( builder.toString().trim() ) );
						  }
						  break;
					  case XMLStreamConstants.SPACE:
					  case XMLStreamConstants.CDATA:
					  case XMLStreamConstants.CHARACTERS:
						  builder.append(parser.getText());
						  break;
					  }
				  }
			  }			  
		});		
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
		
		final Query query = QueryFactory.create(queryString(data.query));
		execution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query);
		inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset);
			
		assertEquals(
				Arrays.toString(data.datasets) + ", " + data.query,
				inMemoryExecution.execAsk(),
				execution.execAsk());
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
			execution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query);
			inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset);
			
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					inMemoryExecution.execDescribe().isIsomorphicWith(execution.execDescribe()));
		} catch (final Throwable error) {
			QueryExecution debugExecution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query);
			StringWriter writer = new StringWriter();
			RDFDataMgr.write(writer, debugExecution.execDescribe(), RDFFormat.NTRIPLES);
			log.debug("JNS\n" + writer);
			
			debugExecution.close();
			debugExecution = QueryExecutionFactory.create(query, memoryDataset);
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
		
		final Query query = QueryFactory.create(queryString(data.query));
		try {
			execution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query);
			inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset);
			
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					inMemoryExecution.execConstruct().isIsomorphicWith(execution.execConstruct()));
		} catch (final Throwable error) {
			QueryExecution debugExecution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query);
			StringWriter writer = new StringWriter();
			RDFDataMgr.write(writer, debugExecution.execConstruct(), RDFFormat.NTRIPLES);
			log.debug("JNS\n" + writer);
			
			debugExecution.close();
			debugExecution = QueryExecutionFactory.create(query, memoryDataset);
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
		
		final Query query = QueryFactory.create(queryString(data.query));
		try {
			assertTrue(
					Arrays.toString(data.datasets) + ", " + data.query,
					ResultSetCompare.isomorphic(
							(execution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query)).execSelect(),
							(inMemoryExecution = QueryExecutionFactory.create(query, memoryDataset)).execSelect()));
		} catch (final Throwable error) {
			QueryExecution debugExecution = null;
			log.debug("JNS\n" + ResultSetFormatter.asText(
					(debugExecution = QueryExecutionFactory.sparqlService(SPARQL_ENDPOINT_URI, query)).execSelect()));
			
			debugExecution.close();
			log.debug("MEM\n" + ResultSetFormatter.asText(
					(debugExecution = (QueryExecutionFactory.create(query, memoryDataset))).execSelect()));
			
			debugExecution.close();
			throw error;
		} 
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
			final String lang = datafileName.endsWith("ttl") ? "TTL" : null;
			memoryModel.read(dataURL, DUMMY_BASE_URI, lang);
		}  
  
		if (data.graphURI != null) {
			DATASET.add(data.graphURI, memoryModel);
		} else {
			DATASET.add(memoryModel);
		}
		commitChanges();
		
		final Iterator<Node> nodes = memoryDataset.asDatasetGraph().listGraphNodes();
		if (nodes != null) {
			while (nodes.hasNext()) {
				final Node graphNode = nodes.next();
				final String graphUri = graphNode.getURI();
				final Model inMemoryNamedModel = memoryDataset.getNamedModel(graphUri);
				assertIsomorphic(inMemoryNamedModel, DATASET.getModel(graphUri), graphUri);		
			}
		}
		
		final Model model = (data.graphURI != null) ? DATASET.getModel(data.graphURI) : DATASET.getModel();
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