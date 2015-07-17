package org.gazzax.labs.solrdf.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

/**
 * SolRDF is the facade class that proxies a remote SolRDF endpoint.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDF {
	final DatasetAccessor remoteDataset;
	final Dataset localDataset;
	
	final SolrServer solr;
	
	/**
	 * SolRDF proxy builder.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	public static class Builder {
		
		private final static String DEFAULT_ENDPOINT = "http://127.0.0.1:8080/solr/store";
		private String graphStoreProtocolEndpointPath = "/rdf-graph-store";
		
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
		 * Sets the Graph Store Protocol Handler url path (e.g. /rdf-graph.store)
		 * 
		 * @param path the Graph Store Protocol Handler url path (e.g. /rdf-graph.store)
		 * @return this builder.
		 */
		public Builder withGraphStoreProtocolEndpointPath(final String path) {
			this.graphStoreProtocolEndpointPath = path;
			return this;
		}
		
		/**
		 * Builds a new SolRDF proxy instance.
		 * 
		 * @return a new SolRDF proxy instance.
		 * @throws IOException in case of I/O failure while connecting with SolRDF.
		 */
		public SolRDF build() throws IOException {
			if (endpoints.isEmpty()) {
				endpoints.add(DEFAULT_ENDPOINT);
			}

			// FIXME: for DatasetAccessor we also need something like LBHttpSolrServer
			try {
				return new SolRDF(
						DatasetAccessorFactory.createHTTP(
								endpoints.iterator().next() +
								graphStoreProtocolEndpointPath),
						(endpoints.size() == 1)
							? new HttpSolrServer(endpoints.iterator().next())
							: new LBHttpSolrServer(endpoints.toArray(new String[endpoints.size()])));
			} catch (final Exception exception) {
				throw new IOException(exception);
			}	
		}
	}
	
	/**
	 * Gets a static reference to 
	 * @return
	 */
	public static Builder newBuilder() {
		return new Builder();
	}	
	
	/**
	 * Builds a new SolRDF proxy with the given {@link DatasetAccessor}.
	 * 
	 * @param dataset the {@link DatasetAccessor} representing the remote endpoint.
	 * @param 
	 */
	SolRDF(final DatasetAccessor dataset, final SolrServer solr) {
		this.remoteDataset = dataset;
		this.localDataset = DatasetFactory.createMem();
		this.solr = solr;
	}

	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 */
	public void add(final List<Statement> statements) {
		remoteDataset.add(model().add(statements));
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 */
	public void add(final String uri, final List<Statement> statements) {
		remoteDataset.add(uri, model(uri).add(statements));
	}
	
	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 */
	public void add(final Statement [] statements) {
		remoteDataset.add(model().add(statements));
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 */
	public void add(final String uri, final Statement [] statements) {
		remoteDataset.add(uri, model(uri).add(statements));
	}	
	
	/**
	 * Adds a statement to the unnamed graph.
	 * 
	 * @param statement the statement.
	 */
	public void add(final Statement statement) {
		remoteDataset.add(model().add(statement));
	}
	
	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 */
	public void add(final String uri, final Statement statement) {
		remoteDataset.add(uri, model(uri).add(statement));
	}		

	/**
	 * Adds a statement to the default graph.
	 * 
	 * @param statement the statement.
	 */
	public void add(final Resource subject, final Property predicate, final RDFNode object) {
		remoteDataset.add(model().add(subject, predicate, object));
	}	

	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 */
	public void add(final String uri, final Resource subject, final Property predicate, final RDFNode object) {
		remoteDataset.add(uri, model(uri).add(subject, predicate, object));
	}		
	
	/**
	 * Adds to the default graph the content of the given URL.
	 * 
	 * @param url the source URL.
	 * @param lang the source data format.
	 */
	public void add(final String url, final String lang) {
		remoteDataset.add(model().read(url, lang));
	}

	/**
	 * Adds to a named graph the content of the given URL.
	 * 
	 * @param uri the graph URI.
	 * @param url the source URL.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final String url, final String lang) {
		remoteDataset.add(uri, model(uri).read(url, lang));
	}
	
	/**
	 * Adds to the default graph the content of the given stream.
	 * 
	 * @param stream the source stream.
	 * @param lang the source data format.
	 */
	public void add(final InputStream stream, final String lang) {
		remoteDataset.add(model().read(stream, null, lang));
	}

	/**
	 * Adds to a named graph the content of the given stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source stream.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final InputStream url, final String lang) {
		remoteDataset.add(uri, model(uri).read(url, null, lang));
	}	
	
	/**
	 * Adds to the default graph the content of the given character stream.
	 * 
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 */
	public void add(final Reader stream, final String lang) {
		remoteDataset.add(model().read(stream, null, lang));
	}

	/**
	 * Adds to a named graph the content of the given character stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final Reader stream, final String lang) {
		remoteDataset.add(uri, model(uri).read(stream, null, lang));
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
	 * @see SolrServer#commit(boolean, boolean)
	 */
	public void commit(final boolean waitFlush, final boolean waitSearcher) throws UnableToCommitException {
		try {
			solr.commit(waitFlush, waitSearcher);
		} catch (final Exception exception) {
			throw new UnableToCommitException(exception);
		}
	}

	/**
	 * Commits pending changes.
	 * 
	 * @param waitFlush blocks until index changes are flushed to disk.
	 * @param waitSearcher blocks until a new searcher is opened and registered as the main query searcher. 
	 * @param softCommit true for issuing a soft commit.
	 * @throws UnableToCommitException in case of commit failure.
	 * @see SolrServer#commit(boolean, boolean, boolean)
	 */
	public void commit(final boolean waitFlush, final boolean waitSearcher, final boolean softCommit) throws UnableToCommitException {
		try {	
			solr.commit(waitFlush, waitSearcher, softCommit);
		} catch (final Exception exception) {
			throw new UnableToCommitException(exception);
		}
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
		
		return model;
	}
	
	/**
	 * Lazy loader for the a local default model.
	 * 
	 * @return the a local default model.
	 */
	Model model() {
		return localDataset.getDefaultModel();
	}
}