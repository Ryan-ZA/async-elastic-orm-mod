package com.rc.gds.interfaces;

public interface GDSSaver {
	
	/**
	 * This function will force the re-saving of any pojo that already have an ID set. Normal behaviour is for all new pojos (pojos with a
	 * blank ID) to be saved recursively, and all pojos that have an ID to be skipped as they are already saved. Not really recommended to
	 * use this function - you should rather save objects directly when you update them!
	 * 
	 * Be very careful of cyclical links in your object graph - it will cause a stack overflow exception when setting this to true.
	 * 
	 * @param recursiveUpdate
	 * @return
	 */
	public abstract GDSSaver forceRecursiveUpdate(boolean recursiveUpdate);
	
	public abstract GDSResult<Key> result(Object pojo);
	
}
