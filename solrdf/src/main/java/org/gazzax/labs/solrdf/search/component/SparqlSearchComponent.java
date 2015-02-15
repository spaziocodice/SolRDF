package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;

import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;

/**
 * A {@link SearchComponent} implementation for executing SPARQL queries.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlSearchComponent extends SearchComponent {
	private static final String DEFAULT_DEF_TYPE = "sparql";
	
	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;

	    try {
			final QParser parser = qParser(request);
	    	final SparqlQuery wrapper = (SparqlQuery) parser.getQuery();
	    	final Query query = wrapper.getQuery();
	    	
	    	final QueryExecution execution = QueryExecutionFactory.create(
					query, 
					DatasetFactory.create(new SolRDFDatasetGraph(request, response, parser)));
			
	    	request.getContext().put(Names.QUERY, query);
			response.add(Names.QUERY, query);
			response.add(Names.QUERY_EXECUTION, execution);			
			
			switch(query.getQueryType()) {
			case Query.QueryTypeAsk:
				response.add(Names.QUERY_RESULT, execution.execAsk());				
				break;
			case Query.QueryTypeSelect: 
				response.add(Names.QUERY_RESULT, execution.execSelect());
				break;
			case Query.QueryTypeDescribe: 
				response.add(Names.QUERY_RESULT, execution.execDescribe());
				break;
			case Query.QueryTypeConstruct: 
				response.add(Names.QUERY_RESULT, execution.execConstruct());
				break;
			default:
				throw new IllegalArgumentException("Unknown query type: " + query.getQueryType());
			}
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