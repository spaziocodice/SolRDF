package org.gazzax.labs.solrdf.search.component;

import static org.gazzax.labs.solrdf.F.isHybrid;
import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.io.IOException;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.cloud.CloudDatasetGraph;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.gazzax.labs.solrdf.log.MessageFactory;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

/**
 * A {@link SearchComponent} implementation for executing SPARQL queries.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlSearchComponent extends SearchComponent {
	private static final String DEFAULT_DEF_TYPE = "sparql";
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(SparqlSearchComponent.class));
	
	private CloudSolrClient server;
	
	@SuppressWarnings("rawtypes")
	@Override
	// FIXME: to be handled better
	public void init(NamedList args) {
		super.init(args);
		final String zkAddress = System.getProperty("zkHost");
		if (isNotNullOrEmptyString(zkAddress)) {
			this.server= new CloudSolrClient(zkAddress);
			this.server.setDefaultCollection("store");
		}
	}
	
	@Override
	public int distributedProcess(final ResponseBuilder responseBuilder) throws IOException {
		doProcess(responseBuilder);
		return super.distributedProcess(responseBuilder);
	}
	
	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
		doProcess(responseBuilder);
	}
	
	/**
	 * Executes the logic of this {@link SearchComponent}.
	 * 
	 * @param responseBuilder the {@link ResponseBuilder} associated with this request.
	 * @throws IOException in case of I/O failure.
	 */
	protected void doProcess(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;

		server.setDefaultCollection(request.getCore().getName());

	    final int start = request.getParams().getInt(CommonParams.START, 0);
		final int rows = request.getParams().getInt(CommonParams.ROWS, 10);
	    
	    try {
			final QParser parser = qParser(request);
	    	final SparqlQuery wrapper = (SparqlQuery) parser.getQuery();
	    	request.getContext().put(Names.HYBRID_MODE, wrapper.isHybrid());
	    	
	    	final Query query = wrapper.getQuery();
	    	final DocListAndSet results = new DocListAndSet();

			final QueryExecution execution = QueryExecutionFactory.create(
	    			query, 
					DatasetFactory.create(
							datasetGraph(
									request, 
									response, 
									parser, 
									wrapper.isHybrid() 
										? new GraphEventConsumer() {
											int currentRow;
											
								    		@Override
											public void afterTripleHasBeenBuilt(final Triple triple, final int docId) {
												currentRow++;
											}

											@Override
											public boolean requireTripleBuild() {
												return currentRow >= start && currentRow < start + rows;
											}

											@Override
											public void onDocSet(final DocSet docSet) {
												results.docSet = results.docSet != null ? results.docSet.union(docSet) : docSet;
											}
										}
										: null)));
	    	
	    	request.getContext().put(Names.QUERY, query);
	    	response.add(Names.QUERY, query);
			response.add(Names.QUERY_EXECUTION, execution);
			
			switch(query.getQueryType()) {
			case Query.QueryTypeAsk:
				response.add(Names.QUERY_RESULT, execution.execAsk());				
				break;
			case Query.QueryTypeSelect: {
				if (wrapper.isHybrid()) {
					
					final ResultSetRewindable resultSet = new PagedResultSet(execution.execSelect(), rows, start);
					while (resultSet.hasNext()) { 
						resultSet.next(); 
					}
			    	
					resultSet.reset();
			    	responseBuilder.setResults(results);
					response.add(Names.QUERY_RESULT, resultSet);					
					response.add(Names.NUM_FOUND, results.docSet.size());					
				} else {
					response.add(Names.QUERY_RESULT, execution.execSelect());					
				}
				break;
			}
			case Query.QueryTypeDescribe: {
				response.add(Names.QUERY_RESULT, execution.execDescribe());
				break;				
			} 
			case Query.QueryTypeConstruct: {				
				response.add(Names.QUERY_RESULT, execution.execConstruct());
				break;
			}
			default:
				final String message = MessageFactory.createMessage(
						MessageCatalog._00111_UNKNOWN_QUERY_TYPE, 
						query.getQueryType());
				LOGGER.error(message);
				throw new IllegalArgumentException(message);
			}
	    } catch (final SyntaxError exception) {
	    	LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
	    	throw new SolrException(ErrorCode.BAD_REQUEST, exception);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_NWS_FAILURE, exception);
			throw new IOException(exception);
		}		
	}
	
	@Override
	public String getDescription() {
		return "sparql";
	}

	@Override
	public String getSource() {
		return "$https://github.com/agazzarini/SolRDF/blob/master/solrdf/src/main/java/org/gazzax/labs/solrdf/search/component/SparqlSearchComponent.java $";
	}
	
	@Override
	public void prepare(final ResponseBuilder responseBuilder) {
		// Nothing to be done here...
	}	
	
	/**
	 * Returns the {@link QParser} associated with this request.
	 * 
	 * @param request the {@link SolrQueryRequest}.
	 * @return the {@link QParser} associated with this request.
	 * @throws SyntaxError in case of syntax errors.
	 */
	QParser qParser(final SolrQueryRequest request) throws SyntaxError {
		return QParser.getParser(
				queryString(request), 
				DEFAULT_DEF_TYPE, 
				request);		
	}
	
	/**
	 * Returns the query string associated with the current request.
	 * 
	 * @param request the current request.
	 * @return the query string associated with the current request.
	 */
	String queryString(final SolrQueryRequest request) {
	    final SolrParams params = request.getParams();
	    String queryString = params.get(CommonParams.Q);

	    if (queryString == null) {
	    	queryString = params.get(CommonParams.QUERY);
	    }
	    
	    if (queryString == null && isHybrid(request)) {
	    	queryString = params.get(Names.DEFAULT_HYBRID_QUERY);
	    }
	    
	    if (queryString == null) {
	    	throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing query");
	    }		
	    
	    return queryString;
	}
	
	/**
	 * Creates an appropriate {@link DatasetGraph} for this SolRDF instance.
	 * 
	 * @param request the current Solr request
	 * @param response the current Solr response.
	 * @param parser the Query parser associated with the current request.
	 * @param consumer a {@link GraphEventConsumer} for this query cycle.
	 * @return an appropriate {@link DatasetGraph} for this SolRDF instance.
	 */
	DatasetGraph datasetGraph(final SolrQueryRequest request, final SolrQueryResponse response, final QParser parser, final GraphEventConsumer consumer) {
		return request.getCore().getCoreDescriptor().getCoreContainer().isZooKeeperAware() 
				? new CloudDatasetGraph(request, response, server)
				: new LocalDatasetGraph(request, response, parser, consumer);
	}
}