package com.rc.gds.interfaces;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.annotations.GwtIncompatible;

import com.rc.gds.GDSClass;

@GwtIncompatible("Cannot create keys on client yet")
public class Key implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public String kind, id;
	public Long version;
	
	public Key() {
	}
	
	public Key(Map<String, Object> map) {
		id = (String) map.get("id");
		kind = (String) map.get("kind");
	}
	
	public Key(Class<?> clazz, String id) {
		this.id = id;
		kind = GDSClass.getKind(clazz);
	}
	
	public Key(String kind, String id) {
		this.id = id;
		this.kind = kind;
	}
	
	public Key(String kind, String id, long version) {
		this(kind, id);
		this.version = version;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (kind == null) {
			if (other.kind != null)
				return false;
		} else if (!kind.equals(other.kind))
			return false;
		return true;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getKind() {
		return kind;
	}
	
	public void setKind(String kind) {
		this.kind = kind;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (id == null ? 0 : id.hashCode());
		result = prime * result + (kind == null ? 0 : kind.hashCode());
		return result;
	}
	
	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("id", id);
		map.put("kind", kind);
		return map;
	}
	
	@Override
	public String toString() {
		return "Key [kind=" + kind + ", id=" + id + ", version=" + version + "]";
	}
	
}
