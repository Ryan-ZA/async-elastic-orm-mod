package com.rc.gds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.Key;

public class PerfTest {
	
	private static GDSImpl getGDS() {
		return new GDSImpl(true, "gdstest");
	}
	
	@Before
	public void testSetup() {
		getGDS().getClient().admin().indices().prepareDelete("*").execute().actionGet();
	}
	
	@After
	public void testCleanup() {
		getGDS().getClient().admin().indices().prepareDelete("*").execute().actionGet();
	}
	
	private void refreshIndex() {
		getGDS().getClient().admin().indices().prepareRefresh().execute().actionGet();
	}
	
	@BeforeClass
	public static void testWarmup() {
		for (int i = 0; i < 10000; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("name", "parent1");
			map.put("__GDS_CLASS_FIELD", "com_rc_gds_TestParent");
			map.put("__GDS_FILTERCLASS_FIELD", new String[] { "com_rc_gds_TestParent" });
			
			IndexResponse r = getGDS().getClient().prepareIndex("test", "test").setSource(map).execute().actionGet();
			assertNotNull(r.getId());
			if (i % 1000 == 0)
				System.out.println("Done " + i);
		}
	}
	
	@Test
	public void testSaveRawMap() {
		
		long time = System.currentTimeMillis();
		
		for (int i = 0; i < 10000; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("name", "parent1");
			map.put("__GDS_CLASS_FIELD", "com_rc_gds_TestParent");
			map.put("__GDS_FILTERCLASS_FIELD", new String[] { "com_rc_gds_TestParent" });
			
			IndexResponse r = getGDS().getClient().prepareIndex("test", "test").setSource(map).execute().actionGet();
			assertNotNull(r.getId());
			if (i % 1000 == 0)
				System.out.println("Done " + i);
		}
		
		System.out.println("testSaveRawMap ellapsed: " + (System.currentTimeMillis() - time));
		
	}
	
	@Test
	public void testSaveGDSerial() {
		long time = System.currentTimeMillis();
		
		for (int i = 0; i < 10000; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent1";
			getGDS().save().result(testParent).now();
			
			assertNotNull(testParent.id);
			if (i % 1000 == 0)
				System.out.println("Done " + i);
		}
		
		System.out.println("testSaveGDSerial ellapsed: " + (System.currentTimeMillis() - time));
	}
	
	@Test
	public void testSaveGDSAsync() {
		long time = System.currentTimeMillis();
		
		// Can only do max 200 at a time because of 200 unit index queue default in ES
		for (int j = 0; j < 100; j++) {
			List<GDSResult<Key>> results = new ArrayList<>();
			for (int i = 0; i < 100; i++) {
				TestParent testParent = new TestParent();
				testParent.name = "parent1";
				results.add(getGDS().save().result(testParent));
			}
			
			boolean success = new GDSBatcher(results.toArray(new GDSResult[0])).onAllComplete().now();
			assertEquals(true, success);
			if (j % 10 == 0)
				System.out.println("Done " + j * 100);
		}
		
		System.out.println("testSaveGDSAsync ellapsed: " + (System.currentTimeMillis() - time));
	}
	
	@Test
	public void testSaveRawXContentBuilder() throws IOException {
		long time = System.currentTimeMillis();
		
		for (int i = 0; i < 10000; i++) {
			XContentBuilder builder = XContentFactory.jsonBuilder()
					.startObject()
					.field("name", "parent1")
					.field("__GDS_CLASS_FIELD", "com_rc_gds_TestParent")
					.array("__GDS_FILTERCLASS_FIELD", "com_rc_gds_TestParent")
					.endObject();
			
			IndexResponse r = getGDS().getClient().prepareIndex("test", "test").setSource(builder).execute().actionGet();
			assertNotNull(r.getId());
			if (i % 1000 == 0)
				System.out.println("Done " + i);
		}
		
		System.out.println("testSaveRawXContentBuilder ellapsed: " + (System.currentTimeMillis() - time));
		
	}
	
	@Test
	public void testLoadGDS() {
		// 1000 as query max results returned is 1000
		for (int i = 0; i < 10000; i++) {
			TestParent testParent = new TestParent();
			testParent.name = "parent1";
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		
		long time = System.currentTimeMillis();
		for (int i = 0; i < 5; i++) {
			List<TestParent> list = getGDS().query(TestParent.class).asList();
			assertEquals(10000, list.size());
		}
		System.out.println("testLoadGDS ellapsed: " + (System.currentTimeMillis() - time));
		
	}
	
	@Test
	public void testLoadRaw() {
		// 1000 as query max results returned is 1000
		for (int i = 0; i < 1000; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("name", "parent1");
			map.put("__GDS_CLASS_FIELD", "com_rc_gds_TestParent");
			map.put("__GDS_FILTERCLASS_FIELD", new String[] { "com_rc_gds_TestParent" });
			
			getGDS().getClient().prepareIndex("test", "test").setSource(map).execute().actionGet();
		}
		
		refreshIndex();
		
		long time = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			SearchResponse response = getGDS().getClient().prepareSearch("test")
					.setQuery(QueryBuilders.matchAllQuery())
					.setSize(1000)
					.execute().actionGet();
			assertEquals(1000, response.getHits().hits().length);
		}
		System.out.println("testLoadRaw ellapsed: " + (System.currentTimeMillis() - time));
		
	}
	
}
