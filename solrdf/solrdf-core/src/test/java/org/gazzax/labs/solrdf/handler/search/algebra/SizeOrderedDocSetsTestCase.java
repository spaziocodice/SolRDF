package org.gazzax.labs.solrdf.handler.search.algebra;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.apache.solr.search.DocSet;
import org.junit.Test;

/**
 * Test case for {@link SizeOrderedDocSets}.
 * 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class SizeOrderedDocSetsTestCase {
	/**
	 * The collected {@link DocSet}s must be ordered by their size.
	 */
	@Test
	public void orderBySize() {
		final DocSet _9 = mock(DocSet.class);
		final DocSet _12 = mock(DocSet.class);
		final DocSet _110 = mock(DocSet.class);
		final DocSet _1090 = mock(DocSet.class);
		final DocSet _10910 = mock(DocSet.class);
		
		when(_9.size()).thenReturn(9);
		when(_12.size()).thenReturn(12);
		when(_110.size()).thenReturn(110);
		when(_1090.size()).thenReturn(1090);
		when(_10910.size()).thenReturn(10910);
		
		final SizeOrderedDocSets docsets = new SizeOrderedDocSets();
		docsets.add(_110);
		docsets.add(_12);
		docsets.add(_1090);
		docsets.add(_9);
		docsets.add(_10910);
		
		final Iterator<DocSet> iterator = docsets.iterator();
		assertSame(_9, iterator.next());
		assertSame(_12, iterator.next());
		assertSame(_110, iterator.next());
		assertSame(_1090, iterator.next());
		assertSame(_10910, iterator.next());
	}
}
