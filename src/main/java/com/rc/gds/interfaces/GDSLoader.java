package com.rc.gds.interfaces;

import java.util.Map;

public interface GDSLoader {
	
	/**
	 * This will fetch a pojo of type clazz with specific id. This method is prefered over fetch(Key)
	 * 
	 * @param clazz
	 *            Class of the object to fetch. Should be the class itself if possible, but superclass or subclass of the object will work
	 *            too.
	 * @param id
	 *            Long id returned from a previous saved pojo
	 * @return
	 */
	public abstract <T> GDSResult<T> fetch(Class<T> clazz, String id);
	
	/**
	 * Will fetch the pojo matching the key.
	 * 
	 * @param key
	 * @return
	 */
	public abstract GDSResult<Object> fetch(Key key);
	
	/**
	 * Will fetch all pojos for keys.
	 * 
	 * @param keys
	 * @return
	 * @throws Exception
	 */
	public abstract GDSResult<Map<Key, Object>> fetchBatch(Iterable<Key> keys) throws Exception;

}
