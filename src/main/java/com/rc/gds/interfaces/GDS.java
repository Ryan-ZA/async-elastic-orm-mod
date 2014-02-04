package com.rc.gds.interfaces;

import org.elasticsearch.client.Client;

public interface GDS {
	
	public abstract String indexFor(String kind);
	
	public abstract String[] indexFor(String[] kinds);
	
	/**
	 * @return A new GDSLoader that can be used to load pojos IFF you have the ID or Key of the pojo.
	 */
	public abstract GDSLoader load();
	
	/**
	 * @return A new GDSSaver that can be used to save any collection of pojos.
	 */
	public abstract GDSSaver save(Object o);
	
	/**
	 * @return A new GDSSaver that can be used to save any collection of pojos.
	 */
	public abstract GDSSaver save();
	
	/**
	 * 
	 * @return A new GDSDelete that can be used to delete pojos from the datastore
	 */
	public abstract GDSDeleter delete();
	
	/**
	 * @param clazz
	 *            The class of pojos to search for. All subclasses of this type will also be searched for.
	 * @return A new parametrized GDSQuery that can be used to search for specific kinds of pojos. Filters and sorting are available.
	 */
	public abstract <T> GDSQuery<T> query(Class<T> clazz);
	
	/**
	 * Begin a transaction that will last until commitTransaction() or rollbackTransaction() is called. You must call one of these when you
	 * have finished the transaction. The transaction will apply to all load() save() and query() calls from this GDS.
	 * 
	 * It is not required to call this to do simple operations - you only need to use this if you wish to commit/rollback all operations
	 * done by this GDS.
	 */
	public abstract void beginTransaction();
	
	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public abstract void commitTransaction();
	
	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public abstract void rollbackTransaction();
	
	public abstract Client getClient();
	
}
