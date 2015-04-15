package org.gazzax.labs.solrdf.handler.search.faceting;

import static org.gazzax.labs.solrdf.Strings.round;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Utility class to compute facets on numeric fields.
 * 
 * The class is a copy of the already existing NumericFacets, which is an inner
 * class of {@link SimpleFacets} with (unfortunately) a default visibility.
 * 
 * @since 1.0
 */
final class NumericFacets {

	static class HashTable {

		static final float LOAD_FACTOR = 0.7f;

		long[] bits; // bits identifying a value
		int[] counts;
		int[] docIDs;
		int mask;
		int size;
		int threshold;

		HashTable() {
			final int capacity = 64; // must be a power of 2
			bits = new long[capacity];
			counts = new int[capacity];
			docIDs = new int[capacity];
			mask = capacity - 1;
			size = 0;
			threshold = (int) (capacity * LOAD_FACTOR);
		}

		private int hash(long v) {
			int h = (int) (v ^ (v >>> 32));
			h = (31 * h) & mask; // * 31 to try to use the whole table, even if
									// values are dense
			return h;
		}

		void add(final int docID, final long value, final int count) {
			if (size >= threshold) {
				rehash();
			}
			
			final int h = hash(value);
			for (int slot = h;; slot = (slot + 1) & mask) {
				if (counts[slot] == 0) {
					bits[slot] = value;
					docIDs[slot] = docID;
					++size;
				} else if (bits[slot] != value) {
					continue;
				}
				counts[slot] += count;
				break;
			}
		}

		private void rehash() {
			final long[] oldBits = bits;
			final int[] oldCounts = counts;
			final int[] oldDocIDs = docIDs;

			final int newCapacity = bits.length * 2;
			bits = new long[newCapacity];
			counts = new int[newCapacity];
			docIDs = new int[newCapacity];
			mask = newCapacity - 1;
			threshold = (int) (LOAD_FACTOR * newCapacity);
			size = 0;

			for (int i = 0; i < oldBits.length; ++i) {
				if (oldCounts[i] > 0) {
					add(oldDocIDs[i], oldBits[i], oldCounts[i]);
				}
			}
		}

	}

	private static class Entry {
		int docID;
		int count;
		long bits;
	}

	public static NamedList<Integer> getCounts(
			final SolrIndexSearcher searcher, 
			final DocSet docs, 
			final String fieldName, 
			final int offset,
			final int limit, 
			final int mincount, 
			final boolean missing, 
			final String sort) throws IOException {
		final SchemaField schemaField = searcher.getSchema().getField(fieldName);
		final FieldType ft = schemaField.getType();
		final NumericType numericType = ft.getNumericType();
		if (numericType == null) {
			throw new IllegalStateException();
		}
		
		final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();

		// 1. accumulate
		final HashTable hashTable = new HashTable();
		final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
		AtomicReaderContext ctx = null;
		FieldCache.Longs longs = null;
		Bits docsWithField = null;
		int missingCount = 0;
		for (final DocIterator iterator = docs.iterator(); iterator.hasNext();) {
			final int doc = iterator.nextDoc();
			if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
				do {
					ctx = ctxIt.next();
				} while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
				assert doc >= ctx.docBase;
				switch (numericType) {
				case LONG:
					longs = FieldCache.DEFAULT.getLongs(ctx.reader(), fieldName, true);
					break;
				case DOUBLE:
					final FieldCache.Doubles doubles = FieldCache.DEFAULT.getDoubles(ctx.reader(), fieldName, true);
					longs = new FieldCache.Longs() {
						@Override
						public long get(int docID) {
							return NumericUtils.doubleToSortableLong(doubles.get(docID));
						}
					};
					break;
				default:
					throw new AssertionError();
				}
				docsWithField = FieldCache.DEFAULT.getDocsWithField(ctx.reader(), fieldName);
			}
			long v = longs.get(doc - ctx.docBase);
			if (v != 0 || docsWithField.get(doc - ctx.docBase)) {
				hashTable.add(doc, v, 1);
			} else {
				++missingCount;
			}
		}

		// 2. select top-k facet values
		final int pqSize = limit < 0 ? hashTable.size : Math.min(offset + limit, hashTable.size);
		final PriorityQueue<Entry> pq;
		if (FacetParams.FACET_SORT_COUNT.equals(sort) || FacetParams.FACET_SORT_COUNT_LEGACY.equals(sort)) {
			pq = new PriorityQueue<Entry>(pqSize) {
				@Override
				protected boolean lessThan(Entry a, Entry b) {
					if (a.count < b.count || (a.count == b.count && a.bits > b.bits)) {
						return true;
					} else {
						return false;
					}
				}
			};
		} else {
			pq = new PriorityQueue<Entry>(pqSize) {
				@Override
				protected boolean lessThan(Entry a, Entry b) {
					return a.bits > b.bits;
				}
			};
		}
		Entry e = null;
		for (int i = 0; i < hashTable.bits.length; ++i) {
			if (hashTable.counts[i] >= mincount) {
				if (e == null) {
					e = new Entry();
				}
				e.bits = hashTable.bits[i];
				e.count = hashTable.counts[i];
				e.docID = hashTable.docIDs[i];
				e = pq.insertWithOverflow(e);
			}
		}

		// 4. build the NamedList
		final ValueSource vs = ft.getValueSource(schemaField, null);
		final NamedList<Integer> result = new NamedList<>();
		final Map<String, Integer> counts = new HashMap<>();
		
		while (pq.size() > 0) {
			final Entry entry = pq.pop();
			final int readerIdx = ReaderUtil.subIndex(entry.docID, leaves);
			final FunctionValues values = vs.getValues(Collections.emptyMap(), leaves.get(readerIdx));
			counts.put(values.strVal(entry.docID - leaves.get(readerIdx).docBase), entry.count);
		}
		
		final Terms terms = searcher.getAtomicReader().terms(fieldName);
		if (terms != null) {
			final String prefixStr = TrieField.getMainValuePrefix(ft);
			final BytesRef prefix;
			if (prefixStr != null) {
				prefix = new BytesRef(prefixStr);
			} else {
				prefix = new BytesRef();
			}
			final TermsEnum termsEnum = terms.iterator(null);
			BytesRef term;
			switch (termsEnum.seekCeil(prefix)) {
			case FOUND:
			case NOT_FOUND:
				term = termsEnum.term();
				break;
			case END:
				term = null;
				break;
			default:
				throw new AssertionError();
			}
			final CharsRef spare = new CharsRef();
			for (int i = 0; i < offset && term != null && StringHelper.startsWith(term, prefix); ++i) {
				term = termsEnum.next();
			}
			for (; term != null && StringHelper.startsWith(term, prefix) && (limit < 0 || result.size() < limit); term = termsEnum.next()) {
				ft.indexedToReadable(term, spare);
				final String termStr = spare.toString();
				final Integer count = counts.get(termStr);
				if (count != null && count > 0) {
					result.add(round(termStr), count);
				}
			}
		}

		if (missing) {
			result.add(null, missingCount);
		}
		return result;
	}

}
