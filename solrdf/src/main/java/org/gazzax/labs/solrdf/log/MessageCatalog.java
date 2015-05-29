package org.gazzax.labs.solrdf.log;

import org.apache.solr.common.params.FacetParams;

/**
 * Message catalog.
 * An interface that (hopefully) enumerates all log messages.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface MessageCatalog {
	String PREFIX = "<SOLRDF";
	
	String _00086_INVALID_RANGE_BOUNDS = PREFIX + "-00086> : Detected invalid facet ranges (low bound: %s, high bound: %s)";
	String _00087_RQ_INFINITE_LOOP_CASE_1 = PREFIX + "-00087> : Detected a range facet infinite loop (gap negative? math overflow?).";
	String _00088_RQ_INFINITE_LOOP_CASE_2 = PREFIX + "-00088> : range facet infinite loop; gap is either zero, or too small relative start/end and caused underflow (gap:%, low bound: %s, high bound: %s)";
	String _00089_INVALID_TARGET_FIELD_FOR_RQ = PREFIX + "-00089> : Unable to range facet on field %s (not a Trie(Double|Date)Field).";
	String _00090_SWITCHING_2_HYB_MODE = PREFIX + "-00090> : Switching to Hybrid mode.";
	String _00091_NULL_QUERY_OR_EXECUTION = PREFIX + "-00091> : Query or QueryExecution cannot be null.";
	String _00092_NEGOTIATED_CONTENT_TYPE = PREFIX + "-00092> : Query type %s, incoming Accept header is %s, applied Content-type is %s";
	String _00093_GSP_REQUEST = PREFIX + "-00093> : Incoming GraphStoreProtocol %s request on %s Graph.";
	String _00094_BULK_LOADER_CT = PREFIX + "-00094> : Content-type of the incoming stream: %s";	
	String _00095_INVALID_CT = PREFIX + "-00095> : Unsupported / Unknown Content-type: %s";	 
	String _00096_SELECTED_BULK_LOADER = PREFIX + "-00096> : Incoming stream with Content-type %s has been associated with %s";	 	
	String _00097_BULK_LOADER_REGISTRY_ENTRY = PREFIX + "-00097> : New Bulk Loader registry entry: %s => %s";	 	
	String _00098_UPDATE_HANDLER_REGISTRY_ENTRY = PREFIX + "-00098> : New Update Loader registry entry: %s => %s";	 	
	String _00099_INVALID_UPDATE_QUERY = PREFIX + "-00099> : Invalid update request: %s";	
	String _00100_INVALID_FACET_METHOD = PREFIX + "-00100> : Invalid facet method %s for facet object query %s";
	String _00101_PREFIX_AND_NUMERIC_FIELD = PREFIX + "-00101> : " + FacetParams.FACET_PREFIX + " is not supported on numeric types.";
	String _00102_UNABLE_TO_COMPUTE_FOQ = PREFIX + "-00102> : Unable to compute facets for object query %s on field %s.";
	String _00103_UNABLE_PARSE_DATEMATH_EXPRESSION = PREFIX + "-00103> : Unable to parse date expression %s";
	String _00104_INCOMING_SPARQL_UPDATE_REQUEST_URL_ENCODED = PREFIX + "-00104> : Incoming SPARQL Update request with URL-encoded parameters.";
	String _00105_INCOMING_SPARQL_UPDATE_REQUEST_DEBUG = PREFIX + "-00105> : Value of update parameter is %s";
	String _00106_INCOMING_SPARQL_UPDATE_REQUEST_URL_ENCODED_TARGET_GRAPH = PREFIX + "-00106> : Applying the incoming Update Request to Graph %s";
	String _00107_INCOMING_SPARQL_UPDATE_REQUEST_URL_ENCODED_ON_UNNAMED_GRAPH = PREFIX + "-00107> : Applying the incoming Update Request to the default Graph.";
	String _00108_INCOMING_SPARQL_UPDATE_REQUEST_USING_POST_DIRECTLY = PREFIX + "-00108> : Incoming SPARQL Update request using POST directly.";
	String _00109_SOLR_QUERY = PREFIX + "-00109> : Query : %s (returned %s of %s total matches)";
	String _00110_INVALID_DATE_VALUE = PREFIX + "-00110> : Unable to convert the given value %s in a valid date.";
	String _00111_UNKNOWN_QUERY_TYPE = PREFIX + "-00111> : Unknown / unsupported query type %s";
	String _00112_GRAPHS_TOTAL_COUNT = PREFIX + "-00112> : SolRDF has %s graphs.";
	String _00113_NWS_FAILURE = PREFIX + "-00113> : An error has been detected. See below for further details.";
	String _00114_ADD_NOT_ALLOWED = PREFIX + "-00114> : Add operation not allowed as this is a read-only view of the underlying graph.";
	String _00115_DELETE_NOT_ALLOWED = PREFIX + "-00114> : Delete operation not allowed as this is a read-only view of the underlying graph.";
	String _00116_CLEAR_NOT_ALLOWED = PREFIX + "-00115> : Clear operation not allowed as this is a read-only view of the underlying graph.";
}