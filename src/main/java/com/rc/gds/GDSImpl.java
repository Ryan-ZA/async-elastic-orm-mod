package com.rc.gds;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import com.rc.es.ESClientHolder;
import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSDeleter;
import com.rc.gds.interfaces.GDSLoader;
import com.rc.gds.interfaces.GDSMultiResult;
import com.rc.gds.interfaces.GDSQuery;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.GDSSaver;
import com.rc.gds.interfaces.Key;

/**
 * 
 * Main class for GDS. A new instance of GDS should be created for every transaction and discarded afterwards.
 * 
 */
public class GDSImpl implements GDS {
	
	private Client client;
	
	//private String cluster;
	
	public GDSImpl(boolean isclient) {
		client = ESClientHolder.getClient(isclient, "gloopsh");
	}
	
	public GDSImpl(boolean isclient, String cluster) {
		client = ESClientHolder.getClient(isclient, cluster);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#indexFor(java.lang.String)
	 */
	@Override
	public String indexFor(String kind) {
		String index = kind.toLowerCase(Locale.US);
		return index;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#indexFor(java.lang.String[])
	 */
	@Override
	public String[] indexFor(String[] kinds) {
		String[] index = new String[kinds.length];
		for (int i = 0; i < kinds.length; i++)
			index[i] = kinds[i].toLowerCase(Locale.US);
		return index;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#load()
	 */
	@Override
	public GDSLoader load() {
		return new GDSLoaderImpl(this);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#save()
	 */
	@Override
	public GDSSaver save() {
		return new GDSSaverImpl(this);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#delete()
	 */
	@Override
	public GDSDeleter delete() {
		return new GDSDeleterImpl(this);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#query(java.lang.Class)
	 */
	@Override
	public <T> GDSQuery<T> query(Class<T> clazz) {
		return new GDSQueryImpl<T>(this, clazz);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#beginTransaction()
	 */
	@Override
	public void beginTransaction() {
		//db.requestStart();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#commitTransaction()
	 */
	@Override
	public void commitTransaction() {
		//db.requestDone();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDS#rollbackTransaction()
	 */
	@Override
	public void rollbackTransaction() {
		//db.requestDone();
	}
	
	public Client getClient() {
		return client;
	}
	
	public static synchronized void clearReflectionCache() {
		GDSClass.clearReflection();
		GDSField.clearReflection();
		ESMapCreator.clearReflection();
	}
	
	public static synchronized void shutdownAllNodes() {
		for (Client client : ESClientHolder.clientMap.values()) {
			client.close();
		}
		for (Node node : ESClientHolder.nodeList) {
			node.stop();
			node.close();
		}
		ESClientHolder.nodeList.clear();
		ESClientHolder.clientMap.clear();
	}
	
	@Override
	public GDSResult<Object> load(Key key) {
		return load().fetch(key);
	}
	
	@Override
	public <T> GDSResult<Key> save(T t) {
		GDSSaver gdsSaver = new GDSSaverImpl(this);
		return gdsSaver.result(t);
	}
	
	@Override
	public <T> GDSResult<Boolean> delete(T t) {
		GDSDeleter gdsDeleter = new GDSDeleterImpl(this);
		return gdsDeleter.delete(t);
	}
	
	@Override
	public <T> GDSResult<T> load(Class<T> clazz, String id) {
		return load().fetch(clazz, id);
	}
	
	@Override
	public <T> GDSResult<List<T>> fetchAll(final Class<T> clazz, String[] ids) {
		final GDSAsyncImpl<List<T>> result = new GDSAsyncImpl<>();
		
		final List<Key> keys = new ArrayList<>();
		for (String id : ids) {
			keys.add(new Key(clazz, id));
		}
		
		try {
			load().fetchBatch(keys).later((map, err) -> {
				if (err != null) {
					result.onSuccess(null, err);
					return;
				}
				
				final List<T> resultList = new ArrayList<>();
				for (Key key : keys) {
					resultList.add((T) map.get(key));
				}
				result.onSuccess(resultList, null);
			});
		} catch (Exception e) {
			result.onSuccess(null, e);
		}
		
		return result;
	}
	
	@Override
	public <T> GDSMultiResult<T> fetchAll(Class<T> clazz) {
		return query(clazz).result();
	}
	
}
