package org.gazzax.labs.solrdf.search.component;

import java.io.IOException;
import java.util.Iterator;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.GraphEventListener;
import org.gazzax.labs.solrdf.graph.SolRDFDatasetGraph;
import org.gazzax.labs.solrdf.search.qparser.SparqlQuery;

import com.carrotsearch.hppc.IntArrayList;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
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
	public void process(final ResponseBuilder responseBuilder) throws IOException {
	    final SolrQueryRequest request = responseBuilder.req;
	    final SolrQueryResponse response = responseBuilder.rsp;
	    
	    try {
			final QParser parser = qParser(request);
	    	final SparqlQuery wrapper = (SparqlQuery) parser.getQuery();
	    	request.getContext().put(Names.HYBRID_MODE, wrapper.isHybrid());
	    	
	    	final Query query = wrapper.getQuery();
	    	final IntArrayList docs = new IntArrayList(50);
	    	final GraphEventListener listener = new GraphEventListener() {
				@Override
				public void afterTripleHasBeenBuilt(final Triple triple, final int docId) {
					docs.add(docId);
				}
			};
	    	
			final QueryExecution execution = QueryExecutionFactory.create(
	    			query, 
					DatasetFactory.create(new SolRDFDatasetGraph(request, response, parser, listener)));
	    	
	    	request.getContext().put(Names.QUERY, query);
	    	

	    	response.add(Names.QUERY, query);
			response.add(Names.QUERY_EXECUTION, execution);
			
			switch(query.getQueryType()) {
			case Query.QueryTypeAsk:
				response.add(Names.QUERY_RESULT, execution.execAsk());				
				break;
			case Query.QueryTypeSelect: {
				final ResultSet resultSet = execution.execSelect();
				
				if (wrapper.isHybrid()) {
					// We need to find a way to (alt)
					// - don't parse the resultset
					// - reuse in the RW the parse results
					// We could set something in the triple match to avoid Triple creation in the (DeepPaging)Iterator
					while (resultSet.hasNext()) { resultSet.next(); }
	
			    	final DocListAndSet results = new DocListAndSet();
			    	results.docSet = new HashDocSet(docs.buffer, 0, docs.elementsCount);	
			    	
			    	responseBuilder.setResults(results);
			    	
			    	execution.close();
			    	final QueryExecution execution1 = QueryExecutionFactory.create(
			    			query, 
							DatasetFactory.create(new SolRDFDatasetGraph(request, response, parser, listener)));
					response.add(Names.QUERY_RESULT, execution1.execSelect());
					response.add(Names.NUM_FOUND, docs.elementsCount);					
				} else {
					response.add(Names.QUERY_RESULT, resultSet);					
				}
				break;
			}
			case Query.QueryTypeDescribe: {
				final Model result = execution.execDescribe();
				// See above
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	final DocListAndSet results = new DocListAndSet();
		    	results.docSet = new HashDocSet(docs.buffer, 0, docs.elementsCount);
		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;				
			}
			case Query.QueryTypeConstruct: {				
				final Model result = execution.execConstruct();
				// See above
				final Iterator<Triple> iterator = result.getGraph().find(Node.ANY, Node.ANY, Node.ANY);
				while (iterator.hasNext()) { iterator.next(); }

		    	final DocListAndSet results = new DocListAndSet();
		    	results.docSet = new HashDocSet(docs.buffer, 0, docs.elementsCount);
		    	responseBuilder.setResults(results);

				response.add(Names.QUERY_RESULT, result);
				break;
			}
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