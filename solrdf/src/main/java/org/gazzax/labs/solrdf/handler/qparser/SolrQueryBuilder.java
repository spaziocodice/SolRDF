package org.gazzax.labs.solrdf.handler.qparser;

import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.gazzax.labs.jena.nosql.fwk.util.Strings;
import org.gazzax.labs.solrdf.Field;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryVisitor;
import com.hp.hpl.jena.sparql.core.Prologue;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;

import static org.gazzax.labs.jena.nosql.fwk.util.NTriples.*;

public class SolrQueryBuilder implements QueryVisitor {

	final Query input;
	private org.apache.lucene.search.BooleanQuery output;
	
	public SolrQueryBuilder(final Query input) {
		this.input = input;
	}

	org.apache.lucene.search.Query buildAndGet() {
		input.visit(this);
		return output;
	}
	
	public void startVisit(final Query query) {
		output = new BooleanQuery();
	}

	public void visitPrologue(Prologue prologue) {
		// Nothing to be done here...
	}

	public void visitResultForm(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitSelectResultForm(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitConstructResultForm(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitDescribeResultForm(Query query) {
	}

	public void visitAskResultForm(Query query) {
	}

	public void visitDatasetDecl(Query query) {
	}

	public void visitQueryPattern(final Query query) {
		final ElementGroup group = (ElementGroup) query.getQueryPattern();
		final Element element = group.getElements().get(0);
		final ElementPathBlock epb = (ElementPathBlock) element;
		final List<TriplePath> paths = epb.getPattern().getList();
		
		for (final TriplePath triplePath : paths) {
			final Node s = triplePath.getSubject();
			final Node p = triplePath.getPredicate();
			final Node o = triplePath.getObject();
			if (s.isConcrete()) {
				output.add(new TermQuery(new Term(Field.S, asNt(s))), Occur.MUST);
			}

			if (p.isConcrete()) {
				output.add(new TermQuery(new Term(Field.P, asNt(p))), Occur.MUST);
			}

			if (o.isConcrete()) {
				if (o.isLiteral()) {
					final String language = o.getLiteralLanguage();
					if (Strings.isNotNullOrEmptyString(language)) {
						output.add(new TermQuery(new Term(Field.LANG, language)), Occur.MUST);
					}
					
					final String literalValue = o.getLiteralLexicalForm(); 
					final RDFDatatype dataType = o.getLiteralDatatype();
					if (dataType != null) {
						final String uri = dataType.getURI();
						if (XSDDatatype.XSDboolean.getURI().equals(uri)) {
							output.add(new TermQuery(new Term(Field.BOOLEAN_OBJECT, literalValue)), Occur.MUST);
						} else if (
								XSDDatatype.XSDint.getURI().equals(uri) ||
								XSDDatatype.XSDinteger.getURI().equals(uri) ||
								XSDDatatype.XSDdecimal.getURI().equals(uri) ||
								XSDDatatype.XSDdouble.getURI().equals(uri) ||
								XSDDatatype.XSDlong.getURI().equals(uri)) {
							output.add(new TermQuery(new Term(Field.NUMERIC_OBJECT, literalValue)), Occur.MUST);
						} else if (
								XSDDatatype.XSDdateTime.equals(uri) || 
								XSDDatatype.XSDdate.equals(uri)) {
							output.add(new TermQuery(new Term(Field.DATE_OBJECT, literalValue)), Occur.MUST);
						} else {
							output.add(new TermQuery(new Term(Field.TEXT_OBJECT, literalValue)), Occur.MUST);
						}
					} else {
						output.add(new TermQuery(new Term(Field.TEXT_OBJECT, asNt(o))), Occur.MUST);
					}				
				} else {
					output.add(new TermQuery(new Term(Field.TEXT_OBJECT, asNt(o))), Occur.MUST);
				}				
			}	
		}
	}

	public void visitGroupBy(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitHaving(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitOrderBy(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitLimit(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitOffset(Query query) {
		// TODO Auto-generated method stub

	}

	public void visitValues(Query query) {
		// TODO Auto-generated method stub

	}

	public void finishVisit(Query query) {
		// TODO Auto-generated method stub

	}

}
