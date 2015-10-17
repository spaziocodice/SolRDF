package org.gazzax.labs.solrdf.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

/**
 * SolRDF is the facade class that proxies a remote SolRDF endpoint.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDF {
	final DatasetAccessor remoteDataset;
	final Dataset localDataset;
	final String sparqlEndpoint;
	final SolrClient solr;

	static class CloseableResultSet implements ResultSet {
		
		private final ResultSet resultset;
		private final QueryExecution execution;
		
		/**
		 * Builds a new {@link ResultSet} wrapping out the given data.
		 * 
		 * @param resultset the actual {@link ResultSet}.
		 * @param execution the {@link QueryExecution} associated with the given {@link ResultSet}.
		 */
		public CloseableResultSet(
				final ResultSet resultset, 
				final QueryExecution execution) {
			this.resultset = resultset;
			this.execution = execution;
		}
		
		@Override
		public void remove() {
			resultset.remove();
		}

		@Override
		public boolean hasNext() {
			return resultset.hasNext();
		}

		@Override
		public QuerySolution next() {
			return resultset.next();
		}

		@Override
		public QuerySolution nextSolution() {
			return resultset.nextSolution();
		}

		@Override
		public Binding nextBinding() {
			return resultset.nextBinding();
		}

		@Override
		public int getRowNumber() {
			return resultset.getRowNumber();
		}

		@Override
		public List<String> getResultVars() {
			return resultset.getResultVars();
		}

		@Override
		public Model getResourceModel() {
			return resultset.getResourceModel();
		}
		
		public void close() {
			execution.close();
		}
	}
	
	/**
	 * SolRDF proxy builder.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	public static class Builder {
		
		private final static String DEFAULT_ENDPOINT = "http://127.0.0.1:8080/solr/store";
		private String graphStoreProtocolEndpointPath = "/rdf-graph-store";
		private String sparqlEndpointPath = "/sparql";
		
		private String zkHost;
		
		private Set<String> endpoints = new HashSet<String>();
		
		/**
		 * Adds a new SolRDF endpoint to this builder.
		 * An endpoint is a running SolRDF node. For example http://127.0.0.1:8080/solr/store
		 *  
		 * @param endpoint the SolRDF endpoint.
		 * @return this builder.
		 */
		public Builder withEndpoint(final String endpoint) {
			endpoints.add(endpoint);
			return this;
		}
		
		/**
		 * Sets the Zookeeper host(s) within the proxy that will be built.
		 * As you can imagine, this will create a proxy for a SolRDF running in SolrCloud mode.
		 * 
		 * TODO: not yet implemented 
		 *  
		 * @param endpoint the SolRDF endpoint.
		 * @return this builder.
		 */
		public Builder withZkHost(final String zkHost) {
			this.zkHost = zkHost;
			throw new UnsupportedOperationException("Not Yet Implemented!");
		}
		
		/**
		 * Sets the Graph Store Protocol Handler url path (defaults to /rdf-graph.store)
		 * 
		 * @param path the Graph Store Protocol Handler url path (e.g. /rdf-graph.store)
		 * @return this builder.
		 */
		public Builder withGraphStoreProtocolEndpointPath(final String path) {
			this.graphStoreProtocolEndpointPath = path;
			return this;
		}
		
		/**
		 * Sets the SPARQL 1.1 Handler url path (defaults to /sparql)
		 * 
		 * @param path the SPARQL 1.1 Handler url path (defaults to /sparql)
		 * @return this builder.
		 */
		public Builder withSPARQLEndpointPath(final String path) {
			this.sparqlEndpointPath = path;
			return this;
		}		
		
		/**
		 * Builds a new SolRDF proxy instance.
		 * 
		 * @return a new SolRDF proxy instance.
		 * @throws UnableToBuildSolRDFClientException in case of build failure.
		 */
		public SolRDF build() throws UnableToBuildSolRDFClientException {
			if (endpoints.isEmpty()) {
				endpoints.add(DEFAULT_ENDPOINT);
			}

			// FIXME: for DatasetAccessor and (HTTP) query execution service we also need something like LBHttpSolrServer
			final String firstEndpointAddress = endpoints.iterator().next();
			try {
				return new SolRDF(
						DatasetAccessorFactory.createHTTP(
								firstEndpointAddress +
								graphStoreProtocolEndpointPath),
						firstEndpointAddress + sparqlEndpointPath,		
						zkHost != null
							? new CloudSolrClient(zkHost)
							: (endpoints.size() == 1)
								? new HttpSolrClient(endpoints.iterator().next())
								: new LBHttpSolrClient(endpoints.toArray(new String[endpoints.size()])));
			} catch (final Exception exception) {
				throw new UnableToBuildSolRDFClientException(exception);
			}	
		}
	}
	
	/**
	 * Creates a new SolRDF proxy builder.
	 * 
	 * @return a new SolRDF proxy builder.
	 */
	public static Builder newBuilder() {
		return new Builder();
	}	
	
	/**
	 * Builds a new SolRDF proxy with the given {@link DatasetAccessor}.
	 * 
	 * @param dataset the {@link DatasetAccessor} representing the remote endpoint.
	 * @param solr the (remote) Solr proxy.
	 */
	SolRDF(
			final DatasetAccessor dataset, 
			final String sparqlEndpointAddress,
			final SolrClient solr) {
		this.remoteDataset = dataset;
		this.localDataset = DatasetFactory.createMem();
		this.solr = solr;
		this.sparqlEndpoint = sparqlEndpointAddress;
	}
	
	/**
	 * Clears the default model on SolRDF.
	 * 
	 * @throws UnableToDeleteException in case the deletion is not possible.
	 */
	public void deleteDefaultGraph() throws UnableToDeleteException {
		try {
			remoteDataset.deleteDefault();
		} catch(final Exception exception) {
			throw new UnableToDeleteException(exception);
		}
	}

	/**
	 * Clears the whole data on SolRDF.
	 * 
	 * @throws UnableToDeleteException in case the deletion is not possible.
	 */
	public void clear() throws UnableToDeleteException {
		try {
			solr.deleteByQuery("*:*");
		} catch(final Exception exception) {
			throw new UnableToDeleteException(exception);
		}
	}

	/**
	 * Clears the default model on SolRDF.
	 * 
	 * @throws UnableToDeleteException in case the deletion is not possible.
	 */
	public void deleteNamedGraph(final String ... graphURIs) throws UnableToDeleteException {
		if (graphURIs == null) {
			return;
		}
		
		try {
			for (final String graphURI : graphURIs) {
				remoteDataset.deleteModel(graphURI);
			}
		} catch(final Exception exception) {
			throw new UnableToDeleteException(exception);
		}
	}

	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param iterator the statements iterator.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final StmtIterator iterator) throws UnableToAddException {
		try {
			remoteDataset.add(model().add(iterator));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param iterator the statements iterator.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final StmtIterator iterator) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).add(iterator));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}

	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final List<Statement> statements) throws UnableToAddException {
		try {
			remoteDataset.add(model().add(statements));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final List<Statement> statements) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).add(statements));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final Statement [] statements) throws UnableToAddException {
		try {
			remoteDataset.add(model().add(statements));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final Statement [] statements) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).add(statements));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}	
	
	/**
	 * Adds a statement to the unnamed graph.
	 * 
	 * @param statement the statement.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final Statement statement) throws UnableToAddException {
		try {
			remoteDataset.add(model().add(statement));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final Statement statement) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).add(statement));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}		

	/**
	 * Adds a statement to the default graph.
	 * 
	 * @param statement the statement.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final Resource subject, final Property predicate, final RDFNode object) throws UnableToAddException {
		try {
			remoteDataset.add(model().add(subject, predicate, object));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}	

	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final Resource subject, final Property predicate, final RDFNode object) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).add(subject, predicate, object));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}		
	
	/**
	 * Adds to the default graph the content of the given URL.
	 * 
	 * @param url the source URL.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String url, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(model().read(url, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}

	/**
	 * Adds to a named graph the content of the given URL.
	 * 
	 * @param uri the graph URI.
	 * @param url the source URL.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final String url, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).read(url, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}
	
	/**
	 * Adds to the default graph the content of the given stream.
	 * 
	 * @param stream the source stream.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final InputStream stream, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(model().read(stream, null, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}

	/**
	 * Adds to a named graph the content of the given stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source stream.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final InputStream url, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).read(url, null, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}	
	
	/**
	 * Adds to the default graph the content of the given character stream.
	 * 
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final Reader stream, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(model().read(stream, null, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}

	/**
	 * Adds to a named graph the content of the given character stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 * @throws UnableToAddException in case of add (local or remote) failure.
	 */
	public void add(final String uri, final Reader stream, final String lang) throws UnableToAddException {
		try {
			remoteDataset.add(uri, model(uri).read(stream, null, lang));
		} catch (final Exception exception) {
			throw new UnableToAddException(exception);
		}
	}		
	
	/**
	 * Executes a SPARQL ASK.
	 * 
	 * @param query the ASK SELECT Query.
	 * @return the {@link ResultSet} that includes matching bindings.
	 * @throws UnableToExecuteQueryException in case of failure before, during or after the query execution.
	 */
	public boolean ask(final String askQuery) throws UnableToExecuteQueryException {
		QueryExecution execution = null;
		try {
			return (execution = execution(askQuery)).execAsk();
		} catch (final Exception exception) {
			throw new UnableToExecuteQueryException(exception);
		} finally {
			execution.close();
		}
	}
	
	/**
	 * Executes a SPARQL SELECT.
	 * 
	 * @param query the SPARQL SELECT Query.
	 * @return the {@link ResultSet} that includes matching bindings.
	 * @throws UnableToExecuteQueryException in case of failure before, during or after the query execution.
	 */
	public CloseableResultSet select(final String selectQuery) throws UnableToExecuteQueryException {
		QueryExecution execution = null;
		try {
			execution = execution(selectQuery);
			return new CloseableResultSet(execution.execSelect(), execution);
		} catch (final Exception exception) {
			if (execution != null) { 
				execution.close();
			}
			throw new UnableToExecuteQueryException(exception);
		} 
	}
	
	/**
	 * Executes a SPARQL CONSTRUCT.
	 * 
	 * @param query the SPARQL CONSTRUCT Query.
	 * @return the {@link Model} that includes the CONSTRUCT result.
	 * @throws UnableToExecuteQueryException in case of failure before, during or after the query execution.
	 */
	public Model construct(final String constructQuery) throws UnableToExecuteQueryException {
		QueryExecution execution = null;
		try {
			return (execution = execution(constructQuery)).execConstruct();
		} catch (final Exception exception) {
			throw new UnableToExecuteQueryException(exception);
		} finally {
			execution.close();
		}
	}	
	
	/**
	 * Executes a SPARQL DESCRIBDE.
	 * 
	 * @param query the SPARQL DESCRIBE Query.
	 * @return the {@link Model} that includes the DESCRIBE result.
	 * @throws UnableToExecuteQueryException in case of failure before, during or after the query execution.
	 */
	public Model describe(final String describeQuery) throws UnableToExecuteQueryException {
		QueryExecution execution = null;
		try {
			return (execution = execution(describeQuery)).execDescribe();
		} catch (final Exception exception) {
			throw new UnableToExecuteQueryException(exception);
		} finally {
			execution.close();
		}
	}		
	
	/**
	 * Returns the {@link Model} representation associated with the graph associated with the given URI.
	 * 
	 * @param graphUri the graph URI.
	 * @return the {@link Model} representation associated with the graph associated with the given URI.
	 */
	public Model getNamedModel(final String graphUri) {
		return remoteDataset.getModel(graphUri);
	}

	/**
	 * Returns the {@link Model} representation associated with the default graph.
	 * 
	 * @return the {@link Model} representation associated with the default graph.
	 */
	public Model getDefaultModel() {
		return remoteDataset.getModel();
	}
	
	/**
	 * Commits pending changes.
	 * 
	 * @throws UnableToCommitException in case of commit failure.
	 */
	public void commit() throws UnableToCommitException {
		try {
			solr.commit();
		} catch (final Exception exception) {
			throw new UnableToCommitException(exception);
		}
	}
	
	/**
	 * Commits pending changes.
	 * 
	 * @param waitFlush blocks until index changes are flushed to disk.
	 * @param waitSearcher blocks until a new searcher is opened and registered as the main query searcher. 
	 * @throws UnableToCommitException in case of commit failure.
	 * @see SolrClient#commit(boolean, boolean)
	 */
	public void commit(final boolean waitFlush, final boolean waitSearcher) throws UnableToCommitException {
		try {
			solr.commit(waitFlush, waitSearcher);
		} catch (final Exception exception) {
			throw new UnableToCommitException(exception);
		}
	}

	/**
	 * The caller does no longer need this client.
	 */
	public void done() {
		try {
			solr.close();
		} catch (final IOException exception) {
			// Ignore
		}
	}
	
	/**
	 * Commits pending changes.
	 * 
	 * @param waitFlush blocks until index changes are flushed to disk.
	 * @param waitSearcher blocks until a new searcher is opened and registered as the main query searcher. 
	 * @param softCommit true for issuing a soft commit.
	 * @throws UnableToCommitException in case of commit failure.
	 * @see SolrClient#commit(boolean, boolean, boolean)
	 */
	public void commit(final boolean waitFlush, final boolean waitSearcher, final boolean softCommit) throws UnableToCommitException {
		try {	
			solr.commit(waitFlush, waitSearcher, softCommit);
		} catch (final Exception exception) {
			throw new UnableToCommitException(exception);
		}
	}
	
	/**
	 * Returns the {@link QueryExecution} associated with the given query.
	 * 
	 * @param query the SPARQL query.
	 * @return the {@link QueryExecution} associated with the given query.
	 */
	QueryExecution execution(final String query) {
		return QueryExecutionFactory.sparqlService(
				sparqlEndpoint, 
				QueryFactory.create(QueryFactory.create(query)));
	}
	
	/**
	 * Lazy loader for a named model.
	 * 
	 * @param uri the model URI.
	 * @return the named model associated with the given URI.
	 */
	Model model(final String uri) {
		Model model = localDataset.getNamedModel(uri);
		if (model == null) {
			model = ModelFactory.createDefaultModel();
			localDataset.addNamedModel(uri, model);
		}
		model.removeAll();
		return model;
	}
	
	/**
	 * Lazy loader for the a local default model.
	 * 
	 * @return the a local default model.
	 */
	Model model() {
		return localDataset.getDefaultModel().removeAll();
	}
}