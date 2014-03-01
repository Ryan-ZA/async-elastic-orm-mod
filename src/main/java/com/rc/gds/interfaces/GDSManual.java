package com.rc.gds.interfaces;

import com.rc.gds.GDSClass;
import com.rc.gds.GDSField;

public class GDSManual<T> {
	
	String kind;
	String id;
	
	public GDSManual(Class<?> clazz, String id) {
		this.id = id;
		kind = GDSClass.getKind(clazz);
	}
	
	public GDSManual(Object o) {
		try {
			this.id = GDSField.getID(o);
			this.kind = GDSClass.getKind(o);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Key getKey() {
		return new Key(kind, id);
	}
	
}
