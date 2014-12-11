package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;

import org.apache.lucene.search.BooleanQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SyntaxError;
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
	    SolrQueryRequest req = rb.req;
	    SolrParams params = req.getParams();
	    SolrQueryResponse rsp = rb.rsp;

		String defType = params.get(QueryParsing.DEFTYPE, "sparql");

	    // get it from the response builder to give a different component a chance
	    // to set it.
	    String queryString = rb.getQueryString();
	    if (queryString == null) {
	      // this is the normal way it's set.
	      queryString = params.get( CommonParams.Q );
	      rb.setQueryString(queryString);
	    }

	    
		try {
			QParser parser = QParser.getParser(rb.getQueryString(), defType, req);
			rb.setQuery(parser.getQuery());
			rb.setQparser(parser);
		} catch (SyntaxError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	    	final SortSpec sort = responseBuilder.getQparser().getSort(true);
	    	final CursorMark cursorMark = new CursorMark(responseBuilder.req.getSchema(), sort);
			execution = QueryExecutionFactory.create(query, DatasetFactory.create(new SolrDatasetGraph(searcher, sort)));
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
