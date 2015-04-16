package org.gazzax.labs.solrdf.handler.search.faceting;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.BoundedTreeSet;

/**
 * A class that generates facet information for a given request. 
 * 
 * There is a similar class within {@link SimpleFacets} that unfortunately has a default visibility and 
 * therefore cannot be used within {@link RDFacets} (being this latter in a different package).
 */
public class PerSegmentSingleValuedFaceting {
	private SolrIndexSearcher searcher;
	private DocSet docs;
	private String fieldName;
	private int offset;
	private int limit;
	private int mincount;
	private boolean missing;
	private String sort;
	private String prefix;

	private Filter baseSet;

	private int nThreads;

	/**
	 * Builds a new {@link PerSegmentSingleValuedFaceting} with the given data.
	 * 
	 * @param searcher the searcher instance.
	 * @param docs the documents (identifiers) set.
	 * @param fieldName the facet fieldname.
	 * @param offset the offset within the facet results.
	 * @param limit the max number of counts to return.
	 * @param mincount the requested minimum number of occurrences that each count must have.
	 * @param missing if the missing facets must be included as an unnamed result.
	 * @param sort the facets sort criterion.
	 * @param prefix the facet prefix.
	 */
	public PerSegmentSingleValuedFaceting(
			final SolrIndexSearcher searcher, 
			final DocSet docs, 
			final String fieldName, 
			final int offset,
			final int limit, 
			final int mincount, 
			final boolean missing, 
			final String sort, 
			final String prefix) {
		this.searcher = searcher;
		this.docs = docs;
		this.fieldName = fieldName;
		this.offset = offset;
		this.limit = limit;
		this.mincount = mincount;
		this.missing = missing;
		this.sort = sort;
		this.prefix = prefix;
	}

	public void setNumThreads(int threads) {
		nThreads = threads;
	}

	/**
	 * Returns the counts of the facet associated with this {@link PerSegmentSingleValuedFaceting} data.
	 * 
	 * @param executor the executor service.
	 * @return the facet counts.
	 * @throws IOException in case of I/O failure.
	 */
	NamedList<Integer> getFacetCounts(final Executor executor) throws IOException {
		final CompletionService<SegFacet> completionService = new ExecutorCompletionService<>(executor);

		baseSet = docs.getTopFilter();

		final List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
		final LinkedList<Callable<SegFacet>> pending = new LinkedList<>();

		int threads = nThreads <= 0 ? Integer.MAX_VALUE : nThreads;

		for (final AtomicReaderContext leave : leaves) {
			final SegFacet segFacet = new SegFacet(leave);
			final Callable<SegFacet> task = new Callable<SegFacet>() {
				@Override
				public SegFacet call() throws Exception {
					segFacet.countTerms();
					return segFacet;
				}
			};

			// TODO: if limiting threads, submit by largest segment first?

			if (--threads >= 0) {
				completionService.submit(task);
			} else {
				pending.add(task);
			}
		}

		// now merge the per-segment results
		PriorityQueue<SegFacet> queue = new PriorityQueue<SegFacet>(leaves.size()) {
			@Override
			protected boolean lessThan(SegFacet a, SegFacet b) {
				return a.tempBR.compareTo(b.tempBR) < 0;
			}
		};

		boolean hasMissingCount = false;
		int missingCount = 0;
		for (int i = 0, c = leaves.size(); i < c; i++) {
			SegFacet seg = null;

			try {
				Future<SegFacet> future = completionService.take();
				seg = future.get();
				if (!pending.isEmpty()) {
					completionService.submit(pending.removeFirst());
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof RuntimeException) {
					throw (RuntimeException) cause;
				} else {
					throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
							"Error in per-segment faceting on field: " + fieldName, cause);
				}
			}

			if (seg.startTermIndex < seg.endTermIndex) {
				if (seg.startTermIndex == -1) {
					hasMissingCount = true;
					missingCount += seg.counts[0];
					seg.pos = 0;
				} else {
					seg.pos = seg.startTermIndex;
				}
				if (seg.pos < seg.endTermIndex) {
					seg.tenum = seg.si.termsEnum();
					seg.tenum.seekExact(seg.pos);
					seg.tempBR = seg.tenum.term();
					queue.add(seg);
				}
			}
		}

		FacetCollector collector;
		if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
			collector = new CountSortedFacetCollector(offset, limit, mincount);
		} else {
			collector = new IndexSortedFacetCollector(offset, limit, mincount);
		}

		BytesRefBuilder val = new BytesRefBuilder();

		while (queue.size() > 0) {
			SegFacet seg = queue.top();

			// we will normally end up advancing the term enum for this segment
			// while still using "val", so we need to make a copy since the
			// BytesRef
			// may be shared across calls.
			val.copyBytes(seg.tempBR);

			int count = 0;

			do {
				count += seg.counts[seg.pos - seg.startTermIndex];

				// TODO: OPTIMIZATION...
				// if mincount>0 then seg.pos++ can skip ahead to the next
				// non-zero entry.
				seg.pos++;
				if (seg.pos >= seg.endTermIndex) {
					queue.pop();
					seg = queue.top();
				} else {
					seg.tempBR = seg.tenum.next();
					seg = queue.updateTop();
				}
			} while (seg != null && val.get().compareTo(seg.tempBR) == 0);

			boolean stop = collector.collect(val.get(), count);
			if (stop)
				break;
		}

		NamedList<Integer> res = collector.getFacetCounts();

		// convert labels to readable form
		FieldType ft = searcher.getSchema().getFieldType(fieldName);
		int sz = res.size();
		for (int i = 0; i < sz; i++) {
			res.setName(i, ft.indexedToReadable(res.getName(i)));
		}

		if (missing) {
			if (!hasMissingCount) {
				missingCount = SimpleFacets.getFieldMissingCount(searcher, docs, fieldName);
			}
			res.add(null, missingCount);
		}

		return res;
	}

	/**
	 * A Segment facet.
	 */
	class SegFacet {
		final AtomicReaderContext context;
		
		/**
		 * Builds a new {@link SegFacet} with the given context.
		 * 
		 * @param context the segment context.
		 */
		SegFacet(final AtomicReaderContext context) {
			this.context = context;
		}

		SortedDocValues si;
		int startTermIndex;
		int endTermIndex;
		int[] counts;

		int pos; // only used when merging
		TermsEnum tenum; // only used when merging

		BytesRef tempBR = new BytesRef();

		void countTerms() throws IOException {
			si = FieldCache.DEFAULT.getTermsIndex(context.reader(), fieldName);

			if (prefix != null) {
				final BytesRefBuilder prefixRef = new BytesRefBuilder();
				prefixRef.copyChars(prefix);
				startTermIndex = si.lookupTerm(prefixRef.get());
				if (startTermIndex < 0) {
					startTermIndex = -startTermIndex - 1;
				}
				prefixRef.append(UnicodeUtil.BIG_TERM);
				endTermIndex = si.lookupTerm(prefixRef.get());
				assert endTermIndex < 0;
				endTermIndex = -endTermIndex - 1;
			} else {
				startTermIndex = -1;
				endTermIndex = si.getValueCount();
			}

			final int nTerms = endTermIndex - startTermIndex;
			if (nTerms > 0) {
				final int[] counts = this.counts = new int[nTerms];
				final DocIdSet idSet = baseSet.getDocIdSet(context, null); 
				final DocIdSetIterator iterator = idSet.iterator();

				int doc;

				if (prefix == null) {
					while ((doc = iterator.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
						counts[1 + si.getOrd(doc)]++;
					}
				} else {
					while ((doc = iterator.nextDoc()) < DocIdSetIterator.NO_MORE_DOCS) {
						int term = si.getOrd(doc);
						int arrIdx = term - startTermIndex;
						if (arrIdx >= 0 && arrIdx < nTerms)
							counts[arrIdx]++;
					}
				}
			}
		}
	}

}

abstract class FacetCollector {
	/*** return true to stop collection */
	public abstract boolean collect(BytesRef term, int count);

	public abstract NamedList<Integer> getFacetCounts();
}

class CountSortedFacetCollector extends FacetCollector {
	private final CharsRefBuilder spare = new CharsRefBuilder();

	final int offset;
	final int limit;
	final int maxsize;
	final BoundedTreeSet<SimpleFacets.CountPair<String, Integer>> queue;

	int min; // the smallest value in the top 'N' values

	public CountSortedFacetCollector(int offset, int limit, int mincount) {
		this.offset = offset;
		this.limit = limit;
		maxsize = limit > 0 ? offset + limit : Integer.MAX_VALUE - 1;
		queue = new BoundedTreeSet<>(maxsize);
		min = mincount - 1; // the smallest value in the top 'N' values
	}

	@Override
	public boolean collect(BytesRef term, int count) {
		if (count > min) {
			// NOTE: we use c>min rather than c>=min as an optimization because
			// we are going in
			// index order, so we already know that the keys are ordered. This
			// can be very
			// important if a lot of the counts are repeated (like zero counts
			// would be).
			spare.copyUTF8Bytes(term);
			queue.add(new SimpleFacets.CountPair<>(spare.toString(), count));
			if (queue.size() >= maxsize) {
				min = queue.last().val;
			}
		}
		return false;
	}

	@Override
	public NamedList<Integer> getFacetCounts() {
		NamedList<Integer> res = new NamedList<>();
		int off = offset;
		int lim = limit >= 0 ? limit : Integer.MAX_VALUE;
		// now select the right page from the results
		for (SimpleFacets.CountPair<String, Integer> p : queue) {
			if (--off >= 0)
				continue;
			if (--lim < 0)
				break;
			res.add(p.key, p.val);
		}
		return res;
	}
}

// This collector expects facets to be collected in index order
class IndexSortedFacetCollector extends FacetCollector {
	private final CharsRefBuilder spare = new CharsRefBuilder();

	int offset;
	int limit;
	final int mincount;
	final NamedList<Integer> res = new NamedList<>();

	public IndexSortedFacetCollector(int offset, int limit, int mincount) {
		this.offset = offset;
		this.limit = limit > 0 ? limit : Integer.MAX_VALUE;
		this.mincount = mincount;
	}

	@Override
	public boolean collect(BytesRef term, int count) {
		if (count < mincount) {
			return false;
		}

		if (offset > 0) {
			offset--;
			return false;
		}

		if (limit > 0) {
			spare.copyUTF8Bytes(term);
			res.add(spare.toString(), count);
			limit--;
		}

		return limit <= 0;
	}

	@Override
	public NamedList<Integer> getFacetCounts() {
		return res;
	}
}
