package org.gazzax.labs.solrdf;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;

public class Main {
	public static void main(String[] args) {
		String a  = 
				"PREFIX ab: <http://learningsparql.com/ns/addressbook#> "+ 
				"SELECT ?first ?last ?workTel "+
				"WHERE "+
				"{ "+
				"  ?s ab:firstName ?first ; "+
				"     ab:lastName ?last . "+
				"  OPTIONAL  "+
				"  { ?s ab:workTel ?workTel . } "+
				"}";
		
		Query q = QueryFactory.create(a);
		System.out.println(Algebra.optimize(Algebra.compile(q)));
	}
}