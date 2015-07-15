package org.gazzax.labs.solrdf.client;

import java.io.InputStream;
import java.io.Reader;
import java.util.List;

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
	 * Adds a statement to the unnamed graph.
	 * 
	 * @param statement the statement.
	 */
	public void add(final Statement statement) {
		dataset.add(model().add(statement));
	}
	
	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 */
	public void add(final String uri, final Statement statement) {
		dataset.add(uri, model(uri).add(statement));
	}		

	/**
	 * Adds a statement to the default graph.
	 * 
	 * @param statement the statement.
	 */
	public void add(Resource subject, Property predicate, RDFNode object) {
		dataset.add(model().add(subject, predicate, object));
	}	

	/**
	 * Adds a statement to a named graph.
	 * 
	 * @param uri the graph URI.
	 * @param statement the statement.
	 */
	public void add(final String uri, Resource subject, Property predicate, RDFNode object) {
		dataset.add(uri, model(uri).add(subject, predicate, object));
	}		
	
	/**
	 * Adds to the default graph the content of the given URL.
	 * 
	 * @param url the source URL.
	 * @param lang the source data format.
	 */
	public void add(final String url, final String lang) {
		dataset.add(model().read(url, lang));
	}

	/**
	 * Adds to a named graph the content of the given URL.
	 * 
	 * @param uri the graph URI.
	 * @param url the source URL.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final String url, final String lang) {
		dataset.add(uri, model(uri).read(url, lang));
	}
	
	/**
	 * Adds to the default graph the content of the given stream.
	 * 
	 * @param stream the source stream.
	 * @param lang the source data format.
	 */
	public void add(final InputStream stream, final String lang) {
		dataset.add(model().read(stream, null, lang));
	}

	/**
	 * Adds to a named graph the content of the given stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source stream.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final InputStream url, final String lang) {
		dataset.add(uri, model(uri).read(url, null, lang));
	}	
	
	/**
	 * Adds to the default graph the content of the given character stream.
	 * 
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 */
	public void add(final Reader stream, final String lang) {
		dataset.add(model().read(stream, null, lang));
	}

	/**
	 * Adds to a named graph the content of the given character stream.
	 * 
	 * @param uri the graph URI.
	 * @param stream the source character stream.
	 * @param lang the source data format.
	 */
	public void add(final String uri, final Reader stream, final String lang) {
		dataset.add(uri, model(uri).read(stream, null, lang));
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