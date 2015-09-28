package org.gazzax.labs.solrdf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;

public class Main { 
	
	public static void main(String[] args) throws IOException {
		String a  = new String(
				Files.readAllBytes(
						java.nio.file.Paths.get( 
								new File("/work/workspaces/rdf/SolRDF/solrdf/solrdf-integration-tests/src/test/resources/LearningSPARQLExamples/ex143.rq").toURI())));
		
		final Query q = QueryFactory.create(a);
		final OpProject project = (OpProject) Algebra.optimize(Algebra.compile(q));
		
		System.out.println(project);
	}
}