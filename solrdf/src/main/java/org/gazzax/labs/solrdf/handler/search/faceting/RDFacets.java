package org.gazzax.labs.solrdf.handler.search.faceting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetCollector;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.DateRangeEndpointCalculator;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.DoubleRangeEndpointCalculator;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.FacetRangeQuery;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.RangeEndpointCalculator;

public class RDFacets extends SimpleFacets {
	static String FACET_RANGE_QUERY = FacetParams.FACET_RANGE + ".q";
	static String FACET_RANGE_QUERY_HINT = FACET_RANGE_QUERY + ".hint";
	static String FACET_RANGE_QUERY_ALIAS = FACET_RANGE_QUERY + ".alias";
	
	public RDFacets(final ResponseBuilder responseBuilder, final DocSet docs, final SolrParams params) {
		super(responseBuilder.req, docs, params);
	}

	@Override
	public NamedList<Object> getFacetRangeCounts() throws IOException, SyntaxError {
	    final NamedList<Object> result = new SimpleOrderedMap<>();

	    final List<FacetRangeQuery> rangeQueries = new ArrayList<FacetRangeQuery>();
		final String[] queries = params.getParams(FACET_RANGE_QUERY);
		if (queries != null && queries.length > 0) {
			final String hint = required.get(FACET_RANGE_QUERY_HINT);
			final String alias = params.get(FACET_RANGE_QUERY_ALIAS);
			final String start = required.get(FacetParams.FACET_RANGE_START);
			final String end = required.get(FacetParams.FACET_RANGE_END);
			final String gapExpression = required.get(FacetParams.FACET_RANGE_GAP);

			// TODO: add other parameters, too
			for (final String query : queries) {
				rangeQueries.add(new FacetRangeQuery(query, alias, hint, start, end, gapExpression));
			}
		} 
		
		int index = 0;
		String facetRangeQuery = null;
		while ( (facetRangeQuery = params.get(FACET_RANGE_QUERY + "." + index)) != null) {
			final String hint = required.get(FACET_RANGE_QUERY_HINT + "." + index);
			final String alias = params.get(FACET_RANGE_QUERY_ALIAS + "." + index);
			final String start = required.get(FacetParams.FACET_RANGE_START + "." + index);
			final String end = required.get(FacetParams.FACET_RANGE_END + "." + index);
			final String gapExpression = required.get(FacetParams.FACET_RANGE_GAP + "." + index);

			// TODO: add other parameters, too
			rangeQueries.add(new FacetRangeQuery(facetRangeQuery, alias, hint, start, end, gapExpression));
		}

		if (rangeQueries.isEmpty()) {
			return result;
		}

		for (final FacetRangeQuery rangeQuery : rangeQueries) {
			final Query query = QParser.getParser(rangeQuery.q, null, req).getQuery();
			final DocSetCollector collector = new DocSetCollector(docs.size() >> 6, docs.size());
			
			req.getSearcher().search(query, docs.getTopFilter(), collector);						
			facetRangeCounts(rangeQuery, result, collector.getDocSet());
		}
		
		return result;
	}

	/**
	 * 
	 * NOTE: The same method already exists in the superclass but unfortunately it has a "default" visibility so it cannot be overriden.
	 * 
	 * @param facetRangeQuery the facet range query. 
	 * @param result the result value object. 
	 * @throws IOException 
	 * @throws SyntaxError
	 */
	@SuppressWarnings("unchecked")
	protected <T extends Comparable<T>> void facetRangeCounts(
			final FacetRangeQuery query, 
			final NamedList<Object> result,
			final DocSet filteredDocSet) throws IOException, SyntaxError {
		final IndexSchema schema = searcher.getSchema();
		
		// TODO: Get a better point about this method, because I think something needs to be changed 
		// (i.e. rewritten because it cannot be overriden)
		parseParams(FacetParams.FACET_RANGE, query.fieldName);

		final SchemaField schemaField = schema.getField(query.fieldName);
		final RangeEndpointCalculator<T> strategy = rangeEndpointCalculator(schemaField);

		final NamedList<Object> facetRange = new SimpleOrderedMap<>();
		final NamedList<Integer> counts = new NamedList<>();
		facetRange.add("counts", counts);

		final T start = strategy.getValue(query.start);
		T end = strategy.getValue(query.end);

		if (end.compareTo(start) < 0) {
			throw new SolrException(
					ErrorCode.BAD_REQUEST,
					"range facet 'end' comes before 'start': " + end + " < " + start);
		}

		final String gap = query.gap;
		facetRange.add("gap", gap);

		final int minCount = params.getFieldInt(query.fieldName, FacetParams.FACET_MINCOUNT, 0);

		final EnumSet<FacetRangeInclude> include = FacetRangeInclude.parseParam(
				params.getFieldParams(query.fieldName, FacetParams.FACET_RANGE_INCLUDE));

		T low = start;

		final boolean useHardEnd = params.getFieldBool(query.fieldName, FacetParams.FACET_RANGE_HARD_END, false);
		while (low.compareTo(end) < 0) {
			T high = strategy.addGap(low, gap);
			if (end.compareTo(high) < 0) {
				if (useHardEnd) {
					high = end;
				} else {
					end = high;
				}
			}
			
			if (high.compareTo(low) < 0) {
				throw new SolrException(
						ErrorCode.BAD_REQUEST,
						"range facet infinite loop (is gap negative? did the math overflow?)");
			}
			
			if (high.compareTo(low) == 0) {
				throw new SolrException(
						ErrorCode.BAD_REQUEST,
						"range facet infinite loop: gap is either zero, or too small relative start/end and caused underflow: "
								+ low + " + " + gap + " = " + high);
			}

			final boolean includeLower = (include
					.contains(FacetRangeInclude.LOWER) || (include
					.contains(FacetRangeInclude.EDGE) && 0 == low
					.compareTo(start)));
			
			final boolean includeUpper = (
					include
					.contains(FacetRangeInclude.UPPER) || (include
					.contains(FacetRangeInclude.EDGE) && 0 == high
					.compareTo(end)));

			final String lowS = strategy.format(low);
			final String highS = strategy.format(high);

			final int count = rangeCount(schemaField, lowS, highS, includeLower, includeUpper, filteredDocSet);
			if (count >= minCount) {
				counts.add(lowS, count);
			}

			low = high;
		}

		facetRange.add("start", strategy.format(start));
		facetRange.add("end", strategy.format(end));
		
		final String[] othersP = params.getFieldParams(query.fieldName, FacetParams.FACET_RANGE_OTHER);
		if (null != othersP && 0 < othersP.length) {
			Set<FacetRangeOther> others = EnumSet.noneOf(FacetRangeOther.class);

			for (final String o : othersP) {
				others.add(FacetRangeOther.get(o));
			}

			// no matter what other values are listed, we don't do
			// anything if "none" is specified.
			if (!others.contains(FacetRangeOther.NONE)) {

				boolean all = others.contains(FacetRangeOther.ALL);
				final String startS = strategy.format(start);
				final String endS = strategy.format(end);

				if (all || others.contains(FacetRangeOther.BEFORE)) {
					// include upper bound if "outer" or if first gap doesn't
					// already include it
					facetRange.add(FacetRangeOther.BEFORE.toString(),
							rangeCount(
									schemaField,
									null,
									startS,
									false,
									(include.contains(FacetRangeInclude.OUTER) || (!(include
											.contains(FacetRangeInclude.LOWER) || include
											.contains(FacetRangeInclude.EDGE))))));

				}
				if (all || others.contains(FacetRangeOther.AFTER)) {
					// include lower bound if "outer" or if last gap doesn't
					// already include it
					facetRange.add(FacetRangeOther.AFTER.toString(),
							rangeCount(
									schemaField,
									endS,
									null,
									(include.contains(FacetRangeInclude.OUTER) || (!(include
											.contains(FacetRangeInclude.UPPER) || include
											.contains(FacetRangeInclude.EDGE)))),
									false));
				}
				if (all || others.contains(FacetRangeOther.BETWEEN)) {
					facetRange.add(FacetRangeOther.BETWEEN.toString(),
							rangeCount(
									schemaField,
									startS,
									endS,
									(include.contains(FacetRangeInclude.LOWER) || include
											.contains(FacetRangeInclude.EDGE)),
									(include.contains(FacetRangeInclude.UPPER) || include
											.contains(FacetRangeInclude.EDGE))));

				}
			}
		}
		result.add(key, facetRange);
	}
		
	@SuppressWarnings("rawtypes")
	RangeEndpointCalculator rangeEndpointCalculator(final SchemaField field) {	
		final FieldType fieldType = field.getType();
		
		if (fieldType instanceof TrieDoubleField) {
			return new DoubleRangeEndpointCalculator(field);			
		} else if (fieldType instanceof TrieDateField) {
			return new DateRangeEndpointCalculator(field);			
		}
		
		throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to range facet on field " + field + " (not a Trie(Double|Date)Field).");			
	}
	
	/**
	 * A similar method exsists on the superclass but it is using a different docset.
	 * 
	 * @param schemaField the target field. 
	 * @param lowBound the low bound.
	 * @param highBound the high bound.
	 * @param includeLowBound if low bound must be included.
	 * @param includeHighBound if high bound must be included.
	 * @param domain the {@link DocSet} resulting from the range query. 
	 * @return the number of occurrences for a given range, for a given range query.
	 * @throws IOException in case of I/O failure.
	 */
	protected int rangeCount(
			final SchemaField schemaField, 
			final String lowBound, 
			final String highBound,
			final boolean includeLowBound, 
			final boolean includeHighBound, 
			final DocSet domain) throws IOException {
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
			return searcher.numDocs(rangeQ, domain);
		}
	}
}