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
	String _00098_UPDATE_HANDLER_REGISTRY_ENTRY = PREFIX + "-00098> : New Bulk Loader registry entry: %s => %s";	 	
	String _00099_INVALID_UPDATE_QUERY = PREFIX + "-00099> : Invalid (empty or null) query.";	
	String _00100_INVALID_FACET_METHOD = PREFIX + "-00100> : Invalid facet method %s for facet object query %s";
	String _00101_PREFIX_AND_NUMERIC_FIELD = PREFIX + "-00101> : " + FacetParams.FACET_PREFIX + " is not supported on numeric types.";
	String _00102_UNABLE_TO_COMPUTE_FOQ = PREFIX + "-00102> : Unable to compute facets for object query %s on field %s.";
}