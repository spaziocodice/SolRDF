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
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;

import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * A {@link SearchComponent} implementation for executing SPARQL queries.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SparqlSearchComponent extends SearchComponent {
	private final static String DEFAULT_DEF_TYPE = "sparql";
	
	@Override
	public void prepare(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrParams params = request.getParams();
	    final SolrQueryResponse response = responseBuilder.rsp;

	    final String queryString = params.get(CommonParams.Q);
	    if (queryString == null) {
	    	throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing query");
	    }
	    
		try {
			final QParser parser = QParser.getParser(
					queryString, 
					params.get(QueryParsing.DEFTYPE, DEFAULT_DEF_TYPE), 
					request);
			responseBuilder.setQuery(parser.getQuery());
			responseBuilder.setQparser(parser);
		} catch (final SyntaxError exception) {
			  throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exception);
		}
	}

	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;
	    final SolrIndexSearcher searcher = request.getSearcher();
	    	    
	    QueryExecution execution = null;
	    try {
	    	final SparqlQuery wrapper = (SparqlQuery) responseBuilder.getQuery();
	    	final Query query = wrapper.getQuery();
	    	
	    	// TODO: can we reuse a DatasetGraph??
			execution = QueryExecutionFactory.create(
					query, 
					DatasetFactory.create(
							new SolrDatasetGraph(searcher, responseBuilder.getQparser().getSort(true))));
			
			// TODO: ASK and CONSTRUCT queries
			response.add(Names.SPARQL_RESULTSET, execution.execSelect());
			response.add(Names.QUERY_EXECUTION, execution);			
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
		return null;
	}

}
