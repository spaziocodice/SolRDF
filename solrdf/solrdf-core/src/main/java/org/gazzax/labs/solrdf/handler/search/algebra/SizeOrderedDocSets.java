package org.gazzax.labs.solrdf.handler.search.algebra;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.solr.search.DocSet;

/**
 * A {@link SortedSet} of {@link DocSet} that orders its elements by their size (desc mode).
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SizeOrderedDocSets extends TreeSet<DocSet> {

	private static final long serialVersionUID = -8542016212841190622L;

	/**
	 * Builds a new {@link SizeOrderedDocSets}.
	 */
	public SizeOrderedDocSets() {
		super(new Comparator<DocSet>() {
			@Override
			public int compare(final DocSet o1, final DocSet o2) {
				return o1.size() - o2.size();
			}
		});
	}
}
