package org.gazzax.labs.solrdf;

/**
 * Enumerative interface for field names used in SOLR schema.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface Field {
	String ID = "id";
	String S = "s";
	String P = "p";
	String C = "c";

	String O = "o";
	String LANG = "o_lang";
	String NUMERIC_OBJECT = "o_n";
	String BOOLEAN_OBJECT = "o_b";
	String DATE_OBJECT = "o_d";
	String TEXT_OBJECT = "o_s";
}