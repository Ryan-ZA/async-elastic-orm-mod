package com.rc.gds.interfaces;

public interface GDSResult<T> {
	
	public GDSResult<T> later(GDSCallback<T> inCallback);
	
	public T now();
	
}
