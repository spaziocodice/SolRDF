package org.gazzax.labs.solrdf.integration;

import java.io.IOException;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.gazzax.labs.solrdf.log.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.LoggerFactory;

/**
 * Supertype layer for all integration tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class IntegrationTestSupertypeLayer {
	protected static final String SOLR_URI = "http://127.0.0.1:8080/solr/store";
	protected static final String SPARQL_ENDPOINT_URI = SOLR_URI + "/sparql";
	protected static final String GRAPH_STORE_ENDPOINT_URI = SOLR_URI + "/rdf-graph-store";
	
	protected final Log log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected static HttpSolrServer solr;
	
	/**
	 * Initilisation procedure for this test case.
	 * 
	 * @throws Exception hopefully never.
	 */
	@BeforeClass
	public static void initClient() {
		solr = new HttpSolrServer(SOLR_URI);
		resetSolRDFXmlResponseParser();
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
}