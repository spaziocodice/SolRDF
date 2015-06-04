package org.gazzax.labs.solrdf.handler.update;

import static org.gazzax.labs.solrdf.F.readCommandFromIncomingStream;
import static org.gazzax.labs.solrdf.Strings.isNotNullOrEmptyString;

import java.io.ByteArrayInputStream;
import java.net.URLDecoder;

import javax.servlet.http.HttpServletRequest;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.gazzax.labs.solrdf.Names;
import org.gazzax.labs.solrdf.graph.GraphEventConsumer;
import org.gazzax.labs.solrdf.graph.cloud.ReadOnlyCloudDatasetGraph;
import org.gazzax.labs.solrdf.graph.standalone.LocalDatasetGraph;
import org.gazzax.labs.solrdf.log.Log;
import org.gazzax.labs.solrdf.log.MessageCatalog;
import org.gazzax.labs.solrdf.log.MessageFactory;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.modify.UsingList;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.update.UpdateRequest;

/**
 * Loads A SPARQL 1.1 Update {@link ContentStream} into Solr.
 * 
 * SPARQL Update is a W3C standard for an RDF update language with SPARQL syntax. 
 * 
 * @see http://www.w3.org/TR/sparql11-update
 * @author Andrea Gazzarini
 * @since 1.0
 */
class Sparql11UpdateRdfDataLoader extends ContentStreamLoader {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(Sparql11UpdateRdfDataLoader.class));
	private static CloudSolrServer CLUSTER;
	static {
		// FIXME: TBHB
		final String zkAddress = System.getProperty("zkHost");
		if (isNotNullOrEmptyString(zkAddress)) {
			CLUSTER = new CloudSolrServer(zkAddress);
			CLUSTER.setDefaultCollection("store");
		}
	}
	
	@Override
	public void load(
			final SolrQueryRequest request, 
			final SolrQueryResponse response,
			final ContentStream stream, 
			final UpdateRequestProcessor processor) throws Exception {
		 
		final SolrParams parameters = request.getParams();
		String updateRequest = parameters.get(Names.UPDATE_PARAMETER_NAME);

		if (isNotNullOrEmptyString(updateRequest)) {
			LOGGER.debug(MessageCatalog._00104_INCOMING_SPARQL_UPDATE_REQUEST_URL_ENCODED);
			updateRequest = URLDecoder.decode(updateRequest, characterEncoding(request));
		} else {
			LOGGER.debug(MessageCatalog._00108_INCOMING_SPARQL_UPDATE_REQUEST_USING_POST_DIRECTLY);
			updateRequest = readCommandFromIncomingStream(stream);
		}
		
		LOGGER.debug(MessageCatalog._00105_INCOMING_SPARQL_UPDATE_REQUEST_DEBUG, updateRequest);
		execute(
				usingList(parameters), 
				updateRequest, 
				datasetGraph(request, response));

	}
	
	/**
	 * Executes a given {@link UpdateRequest} against the given {@link DatasetGraph}.
	 * 
	 * @param updateRequest the update request.
	 * @param datasetGraph the dataset graph.
	 * @param graphUris the target graphs (optional).
	 */
	void execute(final UsingList list, final String updateRequests, final DatasetGraph datasetGraph) {
		try {
			UpdateAction.parseExecute(
					list, 
					datasetGraph, 
					new ByteArrayInputStream(updateRequests.getBytes("UTF-8")));		
		} catch (final Exception exception) {	
			final String message = MessageFactory.createMessage(
					MessageCatalog._00099_INVALID_UPDATE_QUERY, 
					updateRequests);
			LOGGER.error(message, exception);
			throw new SolrException(ErrorCode.BAD_REQUEST, message);
		}	
	}
	 
	/**
	 * Returns the character encoding that will be used to decode the incoming request.
	 * 
	 * @param request the current Solr request.
	 * @return the character encoding that will be used to decode the incoming request.
	 */
	String characterEncoding(final SolrQueryRequest request) {
		final HttpServletRequest httpRequest = (HttpServletRequest) request.getContext().get(Names.HTTP_REQUEST_KEY);
		return httpRequest.getCharacterEncoding() != null ? httpRequest.getCharacterEncoding() : "UTF-8";
	}
	 
	/**
	 * Creates the {@link UsingList} instance (using and using named graphs).
	 * 
	 * @param parameters the current Solr request parameters.
	 * @return the {@link UsingList} instance (using and using named graphs).
	 */
    UsingList usingList(final SolrParams parameters)
    {
        final UsingList result = new UsingList();
		final String [] usingNamedParameters = parameters.getParams(Names.USING_NAMED_GRAPH_URI_PARAMETER_NAME);
		final String [] usingParameters = parameters.getParams(Names.USING_GRAPH_URI_PARAMETER_NAME);
		
		if (usingParameters != null) {
	        for (final String graphUri : usingParameters) {
	            result.addUsing(NodeFactory.createURI(graphUri));
	        }
		}
		
		if (usingNamedParameters != null) {
	        for (final String graphUri : usingNamedParameters)
	        {
	            result.addUsingNamed(NodeFactory.createURI(graphUri));
	        }
		}        
        return result;
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
	DatasetGraph datasetGraph(final SolrQueryRequest request, final SolrQueryResponse response) {
		return CLUSTER != null 
				? new ReadOnlyCloudDatasetGraph(request, response, CLUSTER)
				: new LocalDatasetGraph(request, response);
	}    
}	 