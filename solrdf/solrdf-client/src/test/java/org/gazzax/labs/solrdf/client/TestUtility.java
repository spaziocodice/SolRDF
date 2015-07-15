package org.gazzax.labs.solrdf.client;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;

public class TestUtility {
	
	final static Random RANDOMIZER = new Random();
	
	static List<Statement> STATEMENTS;
	
	/**
	 * Returns a set of sample statements,
	 * 
	 * @return a set of sample statements,
	 * @throws IOException in case of I/O failure.
	 */
	public static List<Statement> sampleStatements() throws IOException {
		if (STATEMENTS == null) {
			STATEMENTS = Lists.newArrayList(
				ModelFactory.createDefaultModel().read(sampleSourceFileURI(sampleSourceFile()), "N-TRIPLES").listStatements());
		} 
		return STATEMENTS;
	}
	
	/**
	 * Returns the sample source file.
	 * 
	 * @return the sample source file.
  	 */
	public static File sampleSourceFile() {
		return new File("../solrdf-integration-tests/src/test/resources/sample_data/bsbm-generated-dataset.nt");
	}		
	
	/**
	 * Returns the given file as URI string.
	 * 
	 * @param file the file.
	 * @return the given file as URI string.
	 */
	public static String sampleSourceFileURI(final File file) {
		return file.toURI().toString();
	}	
	
	/**
	 * Returns an invalid (inexistent) path.
	 *  
	 * @return an invalid (inexistent) path.
	 */
	public static String invalidPath() {
		return String.valueOf(System.currentTimeMillis());
	}
}