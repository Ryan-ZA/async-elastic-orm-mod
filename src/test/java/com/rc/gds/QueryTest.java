package com.rc.gds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryTest {
	
	private static GDSImpl getGDS() {
		return new GDSImpl(true, "gdstest");
	}
	
	@BeforeClass
	public static void testSetup() {
		getGDS().getClient().admin().indices().prepareDelete("*").execute().actionGet();
	}
	
	@After
	public void testCleanup() {
		getGDS().getClient().admin().indices().prepareDelete("*").execute().actionGet();
	}
	
	private void refreshIndex() {
		getGDS().getClient().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@Test
	public void queryTest() {
		int total = 0;
		
		for (int i = 0; i < 100; i++) {
			TestParent testParent = new TestParent();
			TestChild testChild = new TestChild();
			
			testParent.name = "parent" + i;
			testChild.name = "child" + i;
			testParent.testChild = testChild;
			
			getGDS().save().result(testParent).now();
			
			total += i;
		}
		
		refreshIndex();
		
		List<TestParent> result = getGDS().query(TestParent.class).asList();
		int newtotal = 0;
		for (TestParent tp : result) {
			assertEquals("parent", tp.name.substring(0, 6));
			assertEquals("child", tp.testChild.name.substring(0, 5));
			
			newtotal += Integer.parseInt(tp.name.substring(6));
		}
		
		assertEquals(total, newtotal);
	}
	
	@Test
	public void querySameChildTest() {
		int total1 = 0;
		int total2 = 0;
		
		TestChild testChild = new TestChild();
		testChild.name = "child" + 5;
		
		for (int i = 0; i < 100; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent" + i;
			testParent.testChild = testChild;
			
			getGDS().save().result(testParent).now();
			
			total1 += i;
			total2 += 5;
		}
		
		refreshIndex();
		
		List<TestParent> result = getGDS().query(TestParent.class).asList();
		int newtotal1 = 0;
		int newtotal2 = 0;
		
		for (TestParent tp : result) {
			assertEquals("parent", tp.name.substring(0, 6));
			assertEquals("child", tp.testChild.name.substring(0, 5));
			
			newtotal1 += Integer.parseInt(tp.name.substring(6));
			newtotal2 += Integer.parseInt(tp.testChild.name.substring(5));
		}
		
		assertEquals(total1, newtotal1);
		assertEquals(total2, newtotal2);
	}
	
	@Test
	public void querySameChildPolyTest() {
		int total1 = 0;
		int total2 = 0;
		
		TestChild testChild = new TestChild();
		testChild.name = "child" + 5;
		
		TestChild testChildPoly = new TestChildPoly();
		testChildPoly.name = "child" + 15;
		
		for (int i = 0; i < 100; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent" + i;
			if (i % 2 == 0)
				testParent.testChild = testChild;
			else
				testParent.testChild = testChildPoly;
			
			getGDS().save().result(testParent).now();
			
			total1 += i;
			total2 += Integer.parseInt(testParent.testChild.name.substring(5));
		}
		
		refreshIndex();
		
		List<TestParent> result = getGDS().query(TestParent.class).asList();
		int newtotal1 = 0;
		int newtotal2 = 0;
		
		for (TestParent tp : result) {
			assertEquals("parent", tp.name.substring(0, 6));
			assertEquals("child", tp.testChild.name.substring(0, 5));
			
			newtotal1 += Integer.parseInt(tp.name.substring(6));
			newtotal2 += Integer.parseInt(tp.testChild.name.substring(5));
		}
		
		assertEquals(total1, newtotal1);
		assertEquals(total2, newtotal2);
	}
	
	@Test
	public void testQueryDeep() {
		TestChildChild tc1 = new TestChildChild();
		TestChildChild tc2 = new TestChildChild();
		TestChildChild tc3 = new TestChildChild();
		TestChildChild tc4 = new TestChildChild();
		
		tc1.name = "tc1";
		tc1.deepChild = tc2;
		tc2.name = "tc2";
		tc2.deepChild = tc3;
		tc3.name = "tc3";
		tc3.deepChild = tc4;
		tc4.name = "tc4";
		
		for (int i = 0; i < 100; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent" + i;
			testParent.testChild = tc1;
			
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		
		List<TestParent> result = getGDS().query(TestParent.class).asList();
		
		for (TestParent tp : result) {
			
			TestChildChild f1 = (TestChildChild) tp.testChild;
			TestChildChild f2 = f1.deepChild;
			TestChildChild f3 = f2.deepChild;
			TestChildChild f4 = f3.deepChild;
			
			assertEquals(tc1.name, f1.name);
			assertEquals(tc2.name, f2.name);
			assertEquals(tc3.name, f3.name);
			assertEquals(tc4.name, f4.name);
		}
	}
	
	@Test
	public void testQueryDeepDifferentPath() {
		TestChildChild tc1 = new TestChildChild();
		TestChildChild tc2 = new TestChildChild();
		TestChildChild tc3 = new TestChildChild();
		TestChildChild tc4 = new TestChildChild();
		TestChildChild tc5 = new TestChildChild();
		TestChildChild tc6 = new TestChildChild();
		
		tc1.name = "tc1";
		tc1.deepChild = tc2;
		tc2.name = "tc2";
		tc2.deepChild = tc3;
		tc3.name = "tc3";
		
		tc4.name = "tc4";
		tc4.deepChild = tc5;
		tc5.name = "tc5";
		tc5.deepChild = tc6;
		tc6.name = "tc6";
		
		for (int i = 0; i < 100; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent" + i;
			if (i % 2 == 0)
				testParent.testChild = tc1;
			else
				testParent.testChild = tc4;
			
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		
		List<TestParent> result = getGDS().query(TestParent.class).asList();
		
		for (TestParent tp : result) {
			
			TestChildChild f1 = (TestChildChild) tp.testChild;
			TestChildChild f2 = f1.deepChild;
			TestChildChild f3 = f2.deepChild;
			
			assertNotNull(f1.name);
			assertNotNull(f2.name);
			assertNotNull(f3.name);
		}
	}
	
	@Test
	public void testQueryFromMultiple() {
		TestBasicMap basicMap = new TestBasicMap();
		basicMap.name = "map";
		getGDS().save().result(basicMap).now();
		
		TestChild testChild = new TestChild();
		testChild.name = "child";
		getGDS().save().result(testChild).now();
		
		TestChildPoly testChildPoly = new TestChildPoly();
		testChildPoly.name = "childpoly";
		getGDS().save().result(testChildPoly).now();
		
		TestChildChild testChildChild = new TestChildChild();
		testChildChild.name = "childchild";
		getGDS().save().result(testChildChild).now();
		
		refreshIndex();
		
		assertEquals(1, getGDS().query(TestBasicMap.class).asList().size());
		assertEquals(3, getGDS().query(TestChild.class).asList().size());
		assertEquals(1, getGDS().query(TestChildPoly.class).asList().size());
		assertEquals(1, getGDS().query(TestChildChild.class).asList().size());
	}
	
	@Test
	public void testBasicFilter() {
		TestParent testParent = new TestParent();
		testParent.name = "bla";
		getGDS().save().result(testParent).now();
		
		TestParentPoly testParentPoly = new TestParentPoly();
		testParentPoly.name = "blu";
		testParentPoly.testChild = new TestChild();
		testParentPoly.testChild.name = "chi";
		getGDS().save().result(testParentPoly).now();
		
		refreshIndex();
		
		assertEquals(2, getGDS().query(TestParent.class).asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter("testChild", testParentPoly.testChild)
				.asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "bla"))
				.asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "blu"))
				.asList().size());
		assertEquals(0, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "na"))
				.asList().size());
		assertEquals(0, getGDS().query(TestParent.class)
				.filter("testChild", testParentPoly.testChild)
				.filter(QueryBuilders.matchPhraseQuery("name", "bla"))
				.asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter("testChild", testParentPoly.testChild)
				.filter(QueryBuilders.matchPhraseQuery("name", "blu"))
				.asList().size());
	}
	
	@Test
	public void testNumberFilter() {
		for (int i = -5; i < 10; i++) {
			TestParentPoly testParentPoly = new TestParentPoly();
			testParentPoly.name = "test" + i;
			testParentPoly.param1 = i;
			getGDS().save().result(testParentPoly).now();
		}
		
		refreshIndex();
		
		assertEquals(15, getGDS().query(TestParentPoly.class).asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("param1", 2))
				.asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("param1", -2))
				.asList().size());
		assertEquals(3, getGDS().query(TestParent.class)
				.filter(QueryBuilders.rangeQuery("param1").gt(3).lte(6))
				.asList().size());
		assertEquals(3, getGDS().query(TestParent.class)
				.filter(QueryBuilders.rangeQuery("param1").gt(-5).lte(-2))
				.asList().size());
		
	}
	
	@Test
	public void testCommaFilter() {
		for (int i = -5; i < 10; i++) {
			TestParentPoly testParentPoly = new TestParentPoly();
			testParentPoly.name = "test" + i + ",twest" + -i;
			testParentPoly.param1 = i;
			getGDS().save().result(testParentPoly).now();
		}
		
		refreshIndex();
		
		assertEquals(15, getGDS().query(TestParentPoly.class).asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "test-2,twest2"))
				.asList().size());
		assertEquals(0, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "est2,twest-2"))
				.asList().size());
		assertEquals(1, getGDS().query(TestParent.class)
				.filter(QueryBuilders.matchPhraseQuery("name", "test2,twest-2"))
				.asList().size());
		
	}
	
	@Test
	public void testInFilter() {
		TestChildPoly child;
		
		child = new TestChildPoly();
		child.bytes = Arrays.asList(1, 2, 3, 4);
		getGDS().save().result(child).now();
		
		child = new TestChildPoly();
		child.bytes = Arrays.asList(2, 3, 4, 5);
		getGDS().save().result(child).now();
		
		child = new TestChildPoly();
		child.bytes = Arrays.asList(3, 4, 5, 6);
		getGDS().save().result(child).now();
		
		child = new TestChildPoly();
		child.bytes = Arrays.asList(4, 5, 6, 7);
		getGDS().save().result(child).now();
		
		refreshIndex();
		
		int[] test = new int[] { 1 };
		assertEquals(1, getGDS().query(TestChildPoly.class)
				.filter(QueryBuilders.inQuery("bytes", test))
				.asList().size());
		test = new int[] { 1, 2 };
		assertEquals(2, getGDS().query(TestChildPoly.class)
				.filter(QueryBuilders.inQuery("bytes", test))
				.asList().size());
		test = new int[] { 1, 2, 3, 4 };
		assertEquals(4, getGDS().query(TestChildPoly.class)
				.filter(QueryBuilders.inQuery("bytes", test))
				.asList().size());
		test = new int[] { 11, 22, 33, 44 };
		assertEquals(0, getGDS().query(TestChildPoly.class)
				.filter(QueryBuilders.inQuery("bytes", test))
				.asList().size());
	}
	
	@Test
	public void testScriptFilter() {
		for (int i = 0; i < 10; i++) {
			TestEmbedHolder testEmbedHolder = new TestEmbedHolder();
			testEmbedHolder.testEmbed1 = new TestEmbed();
			testEmbedHolder.testEmbed1.x = i;
			testEmbedHolder.testEmbed1.z = 10;
			getGDS().save().result(testEmbedHolder).now();
		}
		
		refreshIndex();
		
		assertEquals(10, getGDS().query(TestEmbedHolder.class).asList().size());
		
		String script = "_source.testEmbed1.x > 5";
		assertEquals(4, getGDS().query(TestEmbedHolder.class)
				.filter(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.scriptFilter(script)))
				.asList().size());
		
		script = "_source.testEmbed1.x * _source.testEmbed1.z < 25";
		assertEquals(3, getGDS().query(TestEmbedHolder.class)
				.filter(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.scriptFilter(script)))
				.asList().size());
		
	}
	
	@Test
	public void testFacet() {
		for (int i = 0; i < 50; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "bla" + i / 10;
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		
		SearchResponse sr = getGDS().getClient().prepareSearch(getGDS().indexFor(GDSClass.getKind(TestParent.class)))
				.addFacet(FacetBuilders.termsFacet("test1").field("name"))
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.get();
		
		TermsFacet termsFacet = sr.getFacets().facet(TermsFacet.class, "test1");
		assertEquals(5, termsFacet.getEntries().size());
		
		assertEquals(0, sr.getHits().getHits().length);
	}
	
	@Test
	public void testFacetChildren() {
		TestChild testChild = new TestChild();
		
		for (int i = 0; i < 50; i++) {
			TestParent testParent = new TestParent();
			testParent.testChild = testChild;
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		
		SearchResponse sr = getGDS().getClient().prepareSearch(getGDS().indexFor(GDSClass.getKind(TestParent.class)))
				.addFacet(FacetBuilders.termsFacet("test1").scriptField("_source.testChild.id"))
				.setQuery(QueryBuilders.matchAllQuery())
				.setSize(0)
				.get();
		
		TermsFacet termsFacet = sr.getFacets().facet(TermsFacet.class, "test1");
		
		assertEquals(1, termsFacet.getEntries().size());
		
		assertEquals(0, sr.getHits().getHits().length);
	}
	
}
