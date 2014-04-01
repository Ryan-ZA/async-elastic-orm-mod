package com.rc.gds.interfaces;

import org.elasticsearch.common.annotations.GwtIncompatible;

import com.rc.gds.GDSClass;
import com.rc.gds.GDSField;

public class GDSManual {
	
	String inner_kind;
	String inner_id;
	
	GDSManual() {
	}
	
	@GwtIncompatible("Cannot be run from GWT")
	public GDSManual(Class<?> clazz, String id) {
		inner_id = id;
		inner_kind = GDSClass.getKind(clazz);
	}
	
	@GwtIncompatible("Cannot be run from GWT")
	public GDSManual(Object o) {
		try {
			inner_id = GDSField.getID(o);
			inner_kind = GDSClass.getKind(o);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	@GwtIncompatible("Cannot be run from GWT")
	public GDSResult<?> get(GDS gds) {
		return gds.load(new Key(inner_kind, inner_id));
	}
	
}
