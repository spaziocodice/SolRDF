package org.gazzax.labs.solrdf.handler.search.faceting;

import java.io.IOException;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SortableDoubleField;
import org.apache.solr.schema.SortableFloatField;
import org.apache.solr.schema.SortableIntField;
import org.apache.solr.schema.SortableLongField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetCollector;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

public class RDFacets extends SimpleFacets {
	static String FACET_RANGE_QUERY = FacetParams.FACET_RANGE + ".query";
	
	private DocSet filteredDocSetForRangeQueries;
	
	public RDFacets(final ResponseBuilder responseBuilder, final DocSet docs, final SolrParams params) {
		super(responseBuilder.req, docs, new ModifiableSolrParams(params));
	}
	
	@Override
	public NamedList<Object> getFacetRangeCounts() throws IOException, SyntaxError {
		// Remove any facet.range because I will use them for "workarounding" the SimpleFacets impl.
		((ModifiableSolrParams)params).remove(FacetParams.FACET_RANGE);

		final String[] queries = params.getParams(FACET_RANGE_QUERY);
		if (queries == null) {
			return super.getFacetCounts();
		}
		
		
		// FIXME : doesn't work with more than one fqr
		for (final String q : queries) {
			final Query query = QParser.getParser(q, null, req).getQuery();
			final DocSetCollector collector = new DocSetCollector(docs.size() >> 6, docs.size());
			
			req.getSearcher().search(query, docs.getTopFilter(), collector);			
			filteredDocSetForRangeQueries = collector.getDocSet();
			
			((ModifiableSolrParams)params).set(FacetParams.FACET_RANGE, "o_n");
		}
		
		return super.getFacetRangeCounts();
	}

	@Override
	protected int rangeCount(
			final SchemaField schemaField, 
			final String lowBound, 
			final String highBound,
			final boolean includeLowBound, 
			final boolean includeHighBound) throws IOException {
		final Query rangeQ = schemaField.getType().getRangeQuery(
				null, 
				schemaField, 
				lowBound, 
				highBound, 
				includeLowBound,
				includeHighBound);
		if (params.getBool(GroupParams.GROUP_FACET, false)) {
			return getGroupedFacetQueryCount(rangeQ);
		} else {
			return searcher.numDocs(rangeQ, filteredDocSetForRangeQueries);
		}
	}
}