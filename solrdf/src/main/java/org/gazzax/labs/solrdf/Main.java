package org.gazzax.labs.solrdf;

import java.io.ByteArrayInputStream;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.modify.UsingList;
import com.hp.hpl.jena.update.Update;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateRequest;

public class Main {
	public static void main(String[] args) {
		
		UsingList l = new UsingList();
		l.addUsingNamed(NodeFactory.createURI("http://BLABALBAL.org"));
		String s = "INSERT DATA { <a> <p> <b> }; INSERT DATA { <a> <p> <b> }; DELETE DATA { <a> <p> <b> }";
		UpdateRequest request = UpdateFactory.read(l, new ByteArrayInputStream(s.getBytes()));
		
		for (Update update : request.getOperations()) {
			System.out.println(update);
		}
	}
}
