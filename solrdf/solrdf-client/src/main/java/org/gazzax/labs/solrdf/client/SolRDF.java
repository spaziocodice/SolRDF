package org.gazzax.labs.solrdf.client;

import java.util.List;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;

/**
 * SolRDF is the facede class that proxies a remote SolRDF endpoint.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SolRDF {
	final DatasetAccessor dataset;
	final Dataset memoryDataset;
	
	/**
	 * Builds a new SolRDF proxy with the given remote address.
	 * 
	 * @param baseAddress the HTTP URL where SolRDF is listening.
	 */
	public SolRDF(final String baseAddress) {
		this(DatasetAccessorFactory.createHTTP(baseAddress + "/rdf-graph-store"));
	}
	
	/**
	 * Builds a new SolRDF proxy with the given {@link DatasetAccessor}.
	 * 
	 * @param dataset the {@link DatasetAccessor} representing the remote endpoint.
	 */
	public SolRDF(final DatasetAccessor dataset) {
		this.dataset = dataset;
		this.memoryDataset = DatasetFactory.createMem();
	}

	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 */
	public void add(final List<Statement> statements) {
		dataset.add(model().add(statements));
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 */
	public void add(final String uri, final List<Statement> statements) {
		dataset.add(uri, model(uri).add(statements));
	}
	
	/**
	 * Adds a given set of statements to the unnamed graph.
	 * 
	 * @param statements the list of statements.
	 */
	public void add(final Statement [] statements) {
		dataset.add(model().add(statements));
	}
	
	/**
	 * Adds a given set of statements to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statements the list of statements.
	 */
	public void add(final String uri, final Statement [] statements) {
		dataset.add(uri, model(uri).add(statements));
	}	
	
	/**
	 * Lazy loader for a named model.
	 * 
	 * @param uri the model URI.
	 * @return the named model associated with the given URI.
	 */
	Model model(final String uri) {
		Model model = memoryDataset.getNamedModel(uri);
		if (model == null) {
			model = ModelFactory.createDefaultModel();
			memoryDataset.addNamedModel(uri, model);
		}
		
		return model;
	}
	
	/**
	 * Lazy loader for the a local default model.
	 * 
	 * @return the a local default model.
	 */
	Model model() {
		return memoryDataset.getDefaultModel();
	}	
}