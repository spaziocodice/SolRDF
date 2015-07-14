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
	
	public static List<Statement> sampleStatements() throws IOException {
		if (STATEMENTS == null) {
			STATEMENTS = Lists.newArrayList(
				ModelFactory.createDefaultModel().read(source(), "N-TRIPLES").listStatements());
		} 
		return STATEMENTS;
	}

	private static String source() {
		return new File("../solrdf-integration-tests/src/test/resources/sample_data/bsbm-generated-dataset.nt").toURI().toString();
	}	
}
