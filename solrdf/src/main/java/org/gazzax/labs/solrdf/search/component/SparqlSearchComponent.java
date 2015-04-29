package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;
import java.util.Iterator;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
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
import org.gazzax.labs.solrdf.graph.cloud.ReadOnlyCloudDatasetGraph;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * A {@link SearchComponent} implementation for executing SPARQL queries.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlSearchComponent extends SearchComponent {
	private static final String DEFAULT_DEF_TYPE = "sparql";
			
	@Override
	public int distributedProcess(ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;

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
							new ReadOnlyCloudDatasetGraph(
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
				final Model result = execution.execDescribe();
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;				
			}
			case Query.QueryTypeConstruct: {				
				final Model result = execution.execConstruct();
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;
			}
			default:
				throw new IllegalArgumentException("Unknown query type: " + query.getQueryType());
			}
	    } catch (final SyntaxError exception) {
	    	throw new SolrException(ErrorCode.BAD_REQUEST, exception);
		} catch (final Exception exception) {
			throw new IOException(exception);
		}		
		return super.distributedProcess(responseBuilder);
	}
	
	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;

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
							new LocalDatasetGraph(
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
				final Model result = execution.execDescribe();
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;				
			}
			case Query.QueryTypeConstruct: {				
				final Model result = execution.execConstruct();
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;
			}
			default:
				throw new IllegalArgumentException("Unknown query type: " + query.getQueryType());
			}
	    } catch (final SyntaxError exception) {
	    	throw new SolrException(ErrorCode.BAD_REQUEST, exception);
		} catch (final Exception exception) {
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
	    
	    if (queryString == null) {
	    	throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing query");
	    }		
	    
	    return queryString;
	}
}