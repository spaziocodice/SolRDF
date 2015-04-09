package org.gazzax.labs.solrdf.handler.search.faceting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.FacetParams.FacetRangeInclude;
import org.apache.solr.common.params.FacetParams.FacetRangeOther;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
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
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.DateRangeEndpointCalculator;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.DoubleRangeEndpointCalculator;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.FacetObjectQuery;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.FacetRangeQuery;
import org.gazzax.labs.solrdf.handler.search.faceting.rq.RangeEndpointCalculator;

/**
 * A class that generates facet information for a given request.
 * Note that it extends the already existing {@link SimpleFacets} in order to reuse 
 * that logic as much as possible.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RDFacets extends SimpleFacets {
	
	static final Executor directExecutor = new Executor() {
		@Override
		public void execute(Runnable task) {
			task.run();
		}
	};

	static final Executor facetExecutor = new ThreadPoolExecutor(
			0,
			Integer.MAX_VALUE, 
			10, 
			TimeUnit.SECONDS, 
			new SynchronousQueue<Runnable>(), 
			new DefaultSolrThreadFactory("facetExecutor"));

	/**
	 * Builds a new {@link RDFacets} with the given data.
	 * 
	 * @param responseBuilder the Solr {@link ResponseBuilder}.
	 * @param docs the {@link DocSet} that delimites the faceting domain. 
	 * @param params the facet parameters.
	 */
	public RDFacets(final ResponseBuilder responseBuilder, final DocSet docs, final SolrParams params) {
		super(responseBuilder.req, docs, params);
	}
	
	@Override
	public NamedList<Object> getFacetCounts() {
		final NamedList<Object> result = super.getFacetCounts();
		result.remove("facet_dates");
		
		try {
			result.add("facet_object_queries", getFacetObjectQueriesCounts());
		} catch (IOException | SyntaxError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		result.add("facet_object_ranges_queries", result.remove("facet_ranges"));
		return result;
	}
	
	@Override
	public NamedList<Object> getFacetDateCounts() {
		return null;
	}
			
	/**
	 * &facet.object.q=
	 * @return
	 * @throws IOException
	 * @throws SyntaxError
	 */
	public NamedList<Object> getFacetObjectQueriesCounts() throws IOException, SyntaxError {
		final NamedList<Object> result = new SimpleOrderedMap<>();

	    final List<FacetObjectQuery> facetObjectQueries = new ArrayList<FacetObjectQuery>();
	    final String[] anonymousQueries = params.getParams(FacetObjectQuery.QUERY);
		int index = 0;
		final SolrParams requiredParams = new RequiredSolrParams(params);
		if (anonymousQueries != null && anonymousQueries.length > 0) {
			for (final String query : anonymousQueries) {
				facetObjectQueries.add(
						FacetObjectQuery.newAnonymousQuery(
							query, 
							index++ == 0 ? params.get(FacetObjectQuery.QUERY_ALIAS) : null, 
							params,
							requiredParams));
			} 
		} 

		index = 0;
		String query = null;
		while ((query = params.get(FacetObjectQuery.QUERY + "." + (++index))) != null) {
			facetObjectQueries.add(
					FacetObjectQuery.newQuery(
							query,
							index, 
							params,
							requiredParams));							
		}

		if (facetObjectQueries.isEmpty()) {
			return result;
		}
		
	    // Passing a negative number for FACET_THREADS implies an unlimited number of threads is acceptable.
	    // Also, a subtlety of directExecutor is that no matter how many times you "submit" a job, it's really
	    // just a method call in that it's run by the calling thread.
	    int maxThreads = req.getParams().getInt(FacetParams.FACET_THREADS, 0);
	    final Executor executor = maxThreads == 0 ? directExecutor : facetExecutor;
	    final Semaphore semaphore = new Semaphore((maxThreads <= 0) ? Integer.MAX_VALUE : maxThreads);
	    final List<Future<NamedList<Object>>> futures = new ArrayList<>(facetObjectQueries.size());

	    try {
	    	for (final FacetObjectQuery foq : facetObjectQueries) {
	//	        parseParams(FacetParams.FACET_FIELD, f);
		        final String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
		        final String workerFacetValue = facetValue;
		        final DocSet workerBase = this.docs;
		        final Callable<NamedList<Object>> callable = new Callable<NamedList<Object>>() {
		        	@Override
		        	public NamedList<Object> call() throws Exception {
		        		try {
		        			final DocSetCollector collector = new DocSetCollector(docs.size() >> 6, docs.size());	
		        			req.getSearcher().search(
		        					QParser.getParser(foq.query(), null, req).getQuery(), 
		        					docs.getTopFilter(), 
		        					collector);			        
		        			
		        			final NamedList<Object> result = new SimpleOrderedMap<>();
		        			// TBU
		        			if(termList != null) {
		        				List<String> terms = StrUtils.splitSmart(termList, ",", true);
		        				result.add(foq.alias(), getListedTermCounts(workerFacetValue, workerBase, terms));
		        			} else {
		        				result.add(foq.alias(), getTermCounts(foq.fieldName(), collector.getDocSet()));
		        			}
		        			return result;
		        		} catch (SolrException se) {
		        			throw se;
		        		} catch (Exception e) {
		        			throw new SolrException(ErrorCode.SERVER_ERROR, "Exception during facet.field: " + workerFacetValue, e);
		        		} finally {
		        			semaphore.release();
		        		}
		        	}
		        };
	
		        final RunnableFuture<NamedList<Object>> runnableFuture = new FutureTask<NamedList<Object>>(callable);
		        semaphore.acquire();
		        executor.execute(runnableFuture);
		        futures.add(runnableFuture);
	    	}
	    	
	    	for (final Future<NamedList<Object>> future : futures) {
	    		result.addAll(future.get());
	    	}
	    	assert semaphore.availablePermits() >= maxThreads;
	    } catch (InterruptedException exception) {
	    	throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while processing facet fields: InterruptedException", exception);
	    } catch (ExecutionException exception) {
	    	final Throwable cause = exception.getCause();
	    	if (cause instanceof RuntimeException) {
	    		throw (RuntimeException) cause;
	    	}
	    	throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while processing facet fields: " + cause.toString(), cause);
	    }		
	    return result;
	}

	@Override
	public NamedList<Object> getFacetRangeCounts() throws IOException, SyntaxError {
	    final NamedList<Object> result = new SimpleOrderedMap<>();

	    final List<FacetRangeQuery> facetRangeQueries = new ArrayList<FacetRangeQuery>();
		final String[] anonymousQueries = params.getParams(FacetRangeQuery.QUERY);

		int index = 0;
		final SolrParams requiredParams = new RequiredSolrParams(params);
		if (anonymousQueries != null && anonymousQueries.length > 0) {
			for (final String query : anonymousQueries) {
				facetRangeQueries.add(
						FacetRangeQuery.newAnonymousQuery(
							query, 
							index++ == 0 ? params.get(FacetRangeQuery.QUERY_ALIAS) : null, 
							params,
							requiredParams));
			} 
		} 
		
		index = 0;
		String query = null;
		while ((query = params.get(FacetRangeQuery.QUERY + "." + (++index))) != null) {
			facetRangeQueries.add(
					FacetRangeQuery.newQuery(
							query,
							index, 
							params,
							requiredParams));							
		}

		if (facetRangeQueries.isEmpty()) {
			return result;
		}
		
	    String threadStr = params.get(CommonParams.THREADS);
	    if (threadStr != null) {
	      threads = Integer.parseInt(threadStr);
	    }

		for (final FacetRangeQuery frq : facetRangeQueries) {
			final DocSetCollector collector = new DocSetCollector(docs.size() >> 6, docs.size());
			
			req.getSearcher().search(
					QParser.getParser(frq.query(), null, req).getQuery(), 
					docs.getTopFilter(), 
					collector);						
			
			facetRangeCounts(frq, result, collector.getDocSet());
		}
		
		return result;
	}

	/**
	 * NOTE: The same method already exists in the superclass but unfortunately it has a "default" visibility so it cannot be overriden.
	 * That's the reason you will find a lot of duplicated code.
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
		final SchemaField schemaField = schema.getField(query.fieldName());
		final RangeEndpointCalculator<T> strategy = rangeEndpointCalculator(schemaField);

		final NamedList<Object> facetRange = new SimpleOrderedMap<>();
		final NamedList<Integer> counts = new NamedList<>();
		facetRange.add("counts", counts);

		final T start = strategy.getValue(query.requiredString(FacetParams.FACET_RANGE_START));
		T end = strategy.getValue(query.requiredString(FacetParams.FACET_RANGE_END));

		if (end.compareTo(start) < 0) {
			throw new SolrException(
					ErrorCode.BAD_REQUEST,
					"range facet 'end' comes before 'start': " + end + " < " + start);
		}

		final String gap = query.requiredString(FacetParams.FACET_RANGE_GAP);
		facetRange.add("gap", gap);

		final int minCount = query.requiredInt(FacetParams.FACET_MINCOUNT);

		final EnumSet<FacetRangeInclude> include = FacetRangeInclude.parseParam(
				query.optionalStrings(
						FacetParams.FACET_RANGE_INCLUDE));

		T low = start;

		while (low.compareTo(end) < 0) {
			T high = strategy.addGap(low, gap);
			if (end.compareTo(high) < 0) {
				if (query.optionalBoolean(FacetParams.FACET_RANGE_HARD_END)) {
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
		
		final String [] otherParameter = query.optionalStrings(FacetParams.FACET_RANGE_OTHER);
		if (otherParameter != null && otherParameter.length > 0) {
			final Set<FacetRangeOther> others = EnumSet.noneOf(FacetRangeOther.class);

			for (final String other : otherParameter) {
				others.add(FacetRangeOther.get(other));
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
		result.add(query.key(), facetRange);
	}
		
	@SuppressWarnings("rawtypes")
	RangeEndpointCalculator rangeEndpointCalculator(final SchemaField field) {	
		final FieldType fieldType = field.getType();
		
		if (fieldType instanceof TrieDoubleField) {
			return new DoubleRangeEndpointCalculator(field);			
		} else if (fieldType instanceof TrieDateField) {
			return new DateRangeEndpointCalculator(field);			
		}
		
		throw new SolrException(
				ErrorCode.BAD_REQUEST, 
				"Unable to range facet on field " + field + " (not a Trie(Double|Date)Field).");			
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