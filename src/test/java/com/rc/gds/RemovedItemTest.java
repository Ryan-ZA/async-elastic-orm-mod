package com.rc.gds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemovedItemTest {

	private static GDSImpl getGDS() {
		return new GDSImpl(false, "gdstest");
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

	@Test
	public void testSimpleRemove() {
		TestParent testParent = new TestParent();
		testParent.testChild = new TestChild();
		
		getGDS().save(testParent).now();
		
		TestParent fetchParent = getGDS().load(TestParent.class, testParent.id).now();
		
		assertEquals(testParent.id, fetchParent.id);
		assertEquals(testParent.testChild.id, fetchParent.testChild.id);
		
		getGDS().delete(testParent.testChild).now();
		
		TestParent fetchParentDel = getGDS().load(TestParent.class, testParent.id).now();
		
		assertNull(fetchParentDel.testChild);
	}
	
	@Test
	public void testListRemove() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentList testParentList = new TestParentList();
		testParentList.name = "testParentList" + num;
		testParentList.testChildList = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentList.testChildList.add(testChild);
		}
		testParentList.testChildArr = testParentList.testChildList.subList(0, 20).toArray(new TestChild[0]);
		
		getGDS().save().result(testParentList).now();
		
		getGDS().delete(testParentList.testChildList.get(15)).now();
		refreshIndex();
		
		List<TestChild> queryList = getGDS().query(TestChild.class).asList();
		assertEquals(99, queryList.size());
		
		TestParentList fetchParent = getGDS().load().fetch(TestParentList.class, testParentList.id).now();
		assertEquals(20, fetchParent.testChildArr.length);
		assertEquals(99, fetchParent.testChildList.size());
		
		assertNull(fetchParent.testChildArr[15]);

		for (int i = 0; i < 99; i++) {
			assertNotNull(fetchParent.testChildList.get(i).name);
		}
		
		assertEquals(99, fetchParent.testChildList.size());
	}
	
}
