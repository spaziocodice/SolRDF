package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;

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
