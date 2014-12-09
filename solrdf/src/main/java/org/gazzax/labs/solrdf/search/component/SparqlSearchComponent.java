package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;

import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

public class SparqlSearchComponent extends SearchComponent {

	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void process(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;
	    final SolrIndexSearcher searcher = request.getSearcher();
	    
	    final SolrIndexSearcher.QueryCommand cmd = responseBuilder.getQueryCommand();
	    
	    final SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
	    
	    QueryExecution execution = null;
	    try {
	    	final SparqlQuery wrapper = (SparqlQuery) cmd.getQuery();
	    	final Query query = wrapper.getQuery();
			execution = QueryExecutionFactory.create(query, DatasetFactory.create(new SolrDatasetGraph(searcher)));
			ResultSet rs = execution.execSelect();
			System.out.println(ResultSetFormatter.asText(rs));
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			if (execution != null) execution.close();
		}
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSource() {
		// TODO Auto-generated method stub
		return null;
	}

}
