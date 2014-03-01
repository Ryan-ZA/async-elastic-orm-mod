package com.rc.gds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rc.gds.TestSubClass.TheSubClass;
import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.Key;

public class BasicTest {

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
	public void testEmbed() {
		TestEmbedHolder embedHolder = new TestEmbedHolder();
		embedHolder.testEmbed1 = new TestEmbed();
		embedHolder.testEmbed1.x = 99;
		embedHolder.testEmbed1.y = 12;
		embedHolder.testEmbed2 = new TestEmbed();
		embedHolder.testEmbed2.x = 1;
		embedHolder.testEmbed2.y = 2;
		embedHolder.testEmbed2.z = 99L;
		
		embedHolder.testEmbed1.insideEmbed = new TestEmbed();
		embedHolder.testEmbed1.insideEmbed.zz = 109L;
		
		getGDS().save().result(embedHolder).now();
		
		refreshIndex();
		TestEmbedHolder loaded = getGDS().query(TestEmbedHolder.class).asList().get(0);
		assertEquals(loaded.testEmbed1.x, 99);
		assertEquals(loaded.testEmbed2.y, new Integer(2));
		assertEquals(loaded.testEmbed2.z, 99L);
		assertEquals(loaded.testEmbed1.insideEmbed.zz, new Long(109L));
	}

	@Test
	public void testEmbedList() {
		TestEmbedListHolder embedHolder = new TestEmbedListHolder();
		
		embedHolder.testEmbedArr = new TestEmbed[25];
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = new TestEmbed();
			embed.x = i;
			embedHolder.testEmbedList.add(embed);
			embedHolder.testEmbedArr[i] = embed;
		}
		
		getGDS().save().result(embedHolder).now();
		
		refreshIndex();
		TestEmbedListHolder loaded = getGDS().query(TestEmbedListHolder.class).asList().get(0);
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = loaded.testEmbedList.get(i);
			assertEquals(i, embed.x);
			assertEquals(null, embed.insideEmbed);
		}
	}

	@Test
	public void testEmbedMap() {
		TestEmbedMapHolder embedHolder = new TestEmbedMapHolder();
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = new TestEmbed();
			embed.x = i;
			embedHolder.testEmbedMap.put("key" + i, embed);
		}
		
		getGDS().save().result(embedHolder).now();
		
		refreshIndex();
		TestEmbedMapHolder loaded = getGDS().query(TestEmbedMapHolder.class).asList().get(0);
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = loaded.testEmbedMap.get("key" + i);
			assertEquals(i, embed.x);
			assertEquals(null, embed.insideEmbed);
		}
	}

	@Test
	public void testList() {

		Random random = new Random();
		int num = random.nextInt();

		TestParentList testParentList = new TestParentList();
		testParentList.name = "testParentList" + num;
		testParentList.testChildList = new ArrayList<>();
		for (int i = 0; i < 2500; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentList.testChildList.add(testChild);
		}
		testParentList.testChildArr = testParentList.testChildList.subList(0, 20).toArray(new TestChild[0]);

		getGDS().save().result(testParentList).now();
		System.out.println("DDD");
		
		refreshIndex();
		
		List<TestChild> queryList = getGDS().query(TestChild.class).asList();
		assertEquals(2500, queryList.size());

		TestParentList fetchParent = getGDS().load().fetch(TestParentList.class, testParentList.id).now();
		assertEquals(20, fetchParent.testChildArr.length);
		assertEquals(2500, fetchParent.testChildList.size());

		for (int i = 0; i < 2500; i++) {
			assertNotNull(fetchParent.testChildList.get(i).name);
		}
		
		assertEquals(2500, fetchParent.testChildList.size());
	}

	@Test
	public void testMap() {
		
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentMap testParentMap = new TestParentMap();
		testParentMap.name = "testParentList" + num;
		testParentMap.testChildMap = new HashMap<String, TestChild>();
		for (int i = 0; i < 25; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentMap.testChildMap.put("key" + i, testChild);
		}
		
		getGDS().save().result(testParentMap).now();
		System.out.println("DD");
		
		TestParentMap fetchParent = getGDS().load().fetch(TestParentMap.class, testParentMap.id).now();
		assertEquals(25, fetchParent.testChildMap.size());
		
		for (int i = 0; i < 25; i++) {
			assertEquals("child" + i, fetchParent.testChildMap.get("key" + i).name);
		}
	}

	@Test
	public void testBasicMaps() {
		TestBasicMap map = new TestBasicMap();
		map.name = "bla";
		map.testMap = new HashMap<>();
		map.testMap.put("Test1", 332.131);
		map.testMap.put("t2", -8.0);

		getGDS().save().result(map).now();

		TestBasicMap map2 = getGDS().load().fetch(TestBasicMap.class, map.id).now();
		
		assertEquals(map.testMap.size(), map2.testMap.size());
		assertEquals(map.testMap.get("Test1"), map2.testMap.get("Test1"));
		assertEquals(map.testMap.get("t2"), map2.testMap.get("t2"));

		map.testMap.put("z", 5.5);
		
		assertEquals(null, map2.testMap.get("z"));
	}

	@Test
	public void testPoly() {
		TestParentPoly parentPoly = new TestParentPoly();
		parentPoly.param1 = 4;
		parentPoly.param2 = true;
		parentPoly.param3 = Long.MAX_VALUE;
		
		TestChildPoly childPoly = new TestChildPoly();
		parentPoly.testChild = childPoly;
		
		getGDS().save().result(parentPoly).now();
		
		refreshIndex();
		TestParentPoly loadPoly = (TestParentPoly) getGDS().query(TestParent.class).asList().get(0);
		assertEquals(parentPoly.param1, loadPoly.param1);
		assertEquals(parentPoly.param2, loadPoly.param2);
		assertEquals(parentPoly.param3, loadPoly.param3);
		assertEquals(parentPoly.testChild.getClass(), loadPoly.testChild.getClass());
		
		TestChildPoly loadChildPoly1 = getGDS().query(TestChildPoly.class).asList().get(0);
		TestChildPoly loadChildPoly2 = (TestChildPoly) getGDS().query(TestChild.class).asList().get(0);
		
		assertEquals(loadChildPoly1.id, loadChildPoly2.id);
		assertEquals(loadChildPoly1.bytes.get(2), loadChildPoly2.bytes.get(2));
	}

	@Test
	public void testQuery() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();
		
		testParent.name = "parent" + num;
		testChild.name = "child" + num;
		testParent.testChild = testChild;
		
		getGDS().save().result(testParent).now();
		
		refreshIndex();

		List<TestChild> list = getGDS().query(TestChild.class)
				//.filter(FilterBuilders.termFilter("name", "child" + num))
				.asList();
		
		assertEquals(1, list.size());
		assertEquals(testChild.id, list.get(0).id);
		assertEquals(testChild.name, list.get(0).name);
	}

	@Test
	public void testQueryMultiple() {
		for (int i = 0; i < 1000; i++) {
			TestParent testParent = new TestParent();
			TestChild testChild = new TestChild();
			
			testParent.name = "parent" + i;
			testChild.name = "child" + i;
			testParent.testChild = testChild;
			
			getGDS().save().result(testParent).now();
		}
		
		refreshIndex();
		List<TestChild> list = getGDS().query(TestChild.class).asList();
		assertEquals(1000, list.size());
		//assertEquals("child12", list.get(12).name);
		//assertEquals("child22", list.get(22).name);
	}

	@Test
	public void testSave() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();

		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;

		getGDS().save().result(testParent).now();
		
		assertNotNull(testParent.id);
		assertNotNull(testChild.id);

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		assertEquals(testParent.name, fetchParent.name);
		assertEquals(testParent.testChild.name, fetchParent.testChild.name);
	}

	@Test
	public void testSubClass() {
		TestSubClass test = new TestSubClass();
		test.name = "parent";
		test.theSubClass = new TheSubClass();
		test.theSubClass.i = 867;

		getGDS().save().result(test).now();

		TestSubClass load = getGDS().load().fetch(TestSubClass.class, test.id).now();
		
		assertNotNull(load.theSubClass);
		
		assertEquals(867, load.theSubClass.i);
	}

	@Test
	public void testMultiSave() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();
		
		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;
		
		getGDS().save().result(testParent).now();
		
		assertNotNull(testParent.id);
		assertNotNull(testChild.id);
		
		for (int i = 0; i < 100; i++) {
			TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
			assertEquals(testParent.name, fetchParent.name);
			assertEquals(testParent.testChild.name, fetchParent.testChild.name);
			
			getGDS().save().result(fetchParent).now();
		}
	}

	@Test
	public void testAlwaysPersist() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();

		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;

		getGDS().save().result(testParent).now();

		testChild.name = "child2";

		getGDS().save().result(testParent).now();

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		assertEquals(testParent.testChild.name, fetchParent.testChild.name);
	}
	
	@Test
	public void testDeep() {
		TestParent testParent = new TestParent();
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

		testParent.name = "parent1";
		testParent.testChild = tc1;

		getGDS().save().result(testParent).now();
		
		assertNotNull(testParent.id);
		assertNotNull(tc1.id);
		assertNotNull(tc2.id);
		assertNotNull(tc3.id);
		assertNotNull(tc4.id);

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		TestChildChild f1 = (TestChildChild) fetchParent.testChild;
		TestChildChild f2 = f1.deepChild;
		TestChildChild f3 = f2.deepChild;
		TestChildChild f4 = f3.deepChild;
		
		assertEquals(tc1.name, f1.name);
		assertEquals(tc2.name, f2.name);
		assertEquals(tc3.name, f3.name);
		assertEquals(tc4.name, f4.name);
	}
	
	@Test
	public void testDeepSharedChild() {
		TestParent testParent = new TestParent();
		
		TestChildChild tcshared = new TestChildChild();
		
		TestChildChild tc1 = new TestChildChild();
		TestChildChild tc2 = new TestChildChild();
		TestChildChild tc3 = new TestChildChild();
		TestChildChild tc4 = new TestChildChild();
		
		tcshared.name = "shared";
		
		tc1.name = "tc1";
		tc1.deepChild = tc2;
		tc2.name = "tc2";
		tc2.deepChild = tc3;
		tc2.deepChild2 = tc4;
		tc3.name = "tc3";
		tc3.deepChild = tc4;
		tc3.deepChild2 = tcshared;
		tc4.name = "tc4";
		tc4.deepChild = tcshared;
		tc4.deepChild2 = tcshared;
		
		testParent.name = "parent1";
		testParent.testChild = tc1;
		
		getGDS().save().result(testParent).now();
		
		assertNotNull(testParent.id);
		assertNotNull(tc1.id);
		assertNotNull(tc2.id);
		assertNotNull(tc3.id);
		assertNotNull(tc4.id);
		
		System.out.println(tcshared.id);

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		TestChildChild f1 = (TestChildChild) fetchParent.testChild;
		TestChildChild f2 = f1.deepChild;
		TestChildChild f3 = f2.deepChild;
		TestChildChild f4 = f3.deepChild;
		TestChildChild fs1 = f4.deepChild;
		TestChildChild fs2 = f4.deepChild2;
		
		assertEquals(tc1.name, f1.name);
		assertEquals(tc2.name, f2.name);
		assertEquals(tc3.name, f3.name);
		assertEquals(tc4.name, f4.name);
		assertEquals(tcshared.name, fs1.name);
		assertEquals(tcshared.name, fs2.name);
	}
	
	@Test
	public void testBidirectional() {
		TestChildChild child1 = new TestChildChild();
		child1.name = "test1";
		
		TestChildChild child2 = new TestChildChild();
		child2.name = "test2";
		
		child1.deepChild = child2;
		child2.deepChild = child1;
		
		getGDS().save().result(child1).now();
		
		assertNotNull(child1.id);
		assertNotNull(child2.id);
		
		TestChildChild fetch1 = getGDS().load().fetch(TestChildChild.class, child1.id).now();
		
		assertEquals(child1.name, fetch1.name);
		assertEquals(child2.name, fetch1.deepChild.name);
		
		TestChildChild fetch2 = getGDS().load().fetch(TestChildChild.class, child2.id).now();
		
		assertEquals(child2.name, fetch2.name);
		assertEquals(child1.name, fetch2.deepChild.name);
	}
	
	@Test
	public void testSameObjectMultipleTimesInList() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentList testParentList = new TestParentList();
		testParentList.name = "testParentList" + num;
		testParentList.testChildList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentList.testChildList.add(testChild);
			testParentList.testChildList.add(testChild);
			testParentList.testChildList.add(testChild);
		}
		
		getGDS().save().result(testParentList).now();
		
		refreshIndex();
		
		TestParentList fetchParent = getGDS().load().fetch(TestParentList.class, testParentList.id).now();
		
		for (int i = 0; i < 30; i++) {
			assertNotNull(fetchParent.testChildList.get(i).name);
		}
		
		assertEquals(30, fetchParent.testChildList.size());
	}
	
	@Test
	public void testSameObjectMultipleTimesInMap() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentMap testParentMap = new TestParentMap();
		testParentMap.name = "testParentList" + num;
		testParentMap.testChildMap = new HashMap<String, TestChild>();
		for (int i = 0; i < 10; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentMap.testChildMap.put("key " + i, testChild);
			testParentMap.testChildMap.put("key2 " + i, testChild);
			testParentMap.testChildMap.put("key3 " + i, testChild);
		}
		
		getGDS().save().result(testParentMap).now();
		
		assertNotNull(testParentMap.id);
		for (TestChild testChild : testParentMap.testChildMap.values())
			assertNotNull(testChild.id);

		TestParentMap fetchParent = getGDS().load().fetch(TestParentMap.class, testParentMap.id).now();
		assertEquals(30, fetchParent.testChildMap.size());

		for (int i = 0; i < 10; i++) {
			assertEquals("child" + i, fetchParent.testChildMap.get("key " + i).name);
			assertEquals("child" + i, fetchParent.testChildMap.get("key2 " + i).name);
			assertEquals("child" + i, fetchParent.testChildMap.get("key3 " + i).name);
		}
	}
	
	@Test
	public void testBatcher() {
		
		TestParent testParent1 = new TestParent();
		GDSResult<Key> result1 = getGDS().save().result(testParent1);
		TestParent testParent2 = new TestParent();
		GDSResult<Key> result2 = getGDS().save().result(testParent2);
		TestParent testParent3 = new TestParent();
		GDSResult<Key> result3 = getGDS().save().result(testParent3);
		TestParent testParent4 = new TestParent();
		GDSResult<Key> result4 = getGDS().save().result(testParent4);
		TestParent testParent5 = new TestParent();
		GDSResult<Key> result5 = getGDS().save().result(testParent5);
		
		GDSResult<Boolean> allResult = new GDSBatcher(result1, result2, result3, result4, result5).onAllComplete();
		boolean success = allResult.now();
		
		assertEquals(true, success);
		
		assertNotNull(testParent1.id);
		assertNotNull(testParent2.id);
		assertNotNull(testParent3.id);
		assertNotNull(testParent4.id);
		assertNotNull(testParent5.id);
	}
	
	@Test
	public void testVersionGoodUpdates() {
		TestVersionedObject testVersionedObject = new TestVersionedObject();
		testVersionedObject.name = "one";
		Key key1 = getGDS().save(testVersionedObject).now();
		
		assertNotNull(testVersionedObject.id);
		assertEquals(1, testVersionedObject.ver);
		
		testVersionedObject.name = "two";
		Key key2 = getGDS().save(testVersionedObject).now();
		
		assertEquals(2, testVersionedObject.ver);
		assertEquals(key1.id, key2.id);
	}
	
	@Test(expected = VersionConflictEngineException.class)
	public void testVersionBadUpdates() throws Throwable {
		try {
			TestVersionedObject testVersionedObject = new TestVersionedObject();
			testVersionedObject.name = "one";
			Key key1 = getGDS().save(testVersionedObject).now();
			
			assertNotNull(testVersionedObject.id);
			assertEquals(1, testVersionedObject.ver);
			
			testVersionedObject.name = "two";
			Key key2 = getGDS().save(testVersionedObject).now();
			
			assertEquals(2, testVersionedObject.ver);
			assertEquals(key1.id, key2.id);
			
			testVersionedObject.ver = 1;
			getGDS().save(testVersionedObject).now();
		} catch (RemoteTransportException ex) {
			throw ex.getCause();
		}
	}
	
	@Test
	public void testPreSave() {
		for (int i = 0; i < 500; i++) {
			TestPreSave preSave = new TestPreSave();
			TestPreSave.staticTestChild = new TestChild();
			assertNull(TestPreSave.staticTestChild.id);
			getGDS().save(preSave).now();
			assertNotNull(TestPreSave.staticTestChild.id);
		}
	}
	
	@Test
	public void testPostSave() {
		for (int i = 0; i < 500; i++) {
			TestPostSave postSave = new TestPostSave();
			TestPostSave.staticTestChild = new TestChild();
			assertNull(TestPostSave.staticTestChild.id);
			getGDS().save(postSave).now();
			assertNotNull(TestPostSave.staticTestChild.id);
		}
	}

}
