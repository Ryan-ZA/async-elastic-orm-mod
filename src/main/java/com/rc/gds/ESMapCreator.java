package com.rc.gds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;

class ESMapCreator {
	
	static Set<String> createdKinds = new HashSet<>();
	static Set<String> createdIndexes = new HashSet<>();
	
	static void ensureIndexCreated(GDSImpl gds, Class<?> clazz) {
		if (gds == null)
			return;

		for (String kind : GDSClass.getKinds(clazz)) {
			if (createdKinds.add(kind)) {
				String type = GDSClass.getKind(clazz);
				String index = gds.indexFor(type);
				if (gds.getClient().admin().indices().prepareExists(index).get().isExists()) {
					boolean success = gds.getClient().admin().indices().preparePutMapping(index).setType(type).setSource(createMap(gds, clazz)).get().isAcknowledged();
					System.out.println(success + ": Added mapping for index " + index + " using class " + kind);
				} else {
					CreateIndexRequestBuilder r = gds.getClient().admin().indices().prepareCreate(index).addMapping(type, createMap(gds, clazz));
					boolean success = r.get().isAcknowledged();
					System.out.println(success + ": Created index " + index + " using class " + kind);
				}
				
			}
		}
	}
	
	private static Map<String, Object> createMap(GDSImpl gds, Class<?> clazz) {
		try {
			Map<String, Object> mapping = new HashMap<String, Object>();
			Map<String, GDSField> gdsMap = GDSField.createMapFromClass(null, clazz);
			for (GDSField field : gdsMap.values()) {
				if (field.fieldName == GDSField.GDS_ID_FIELD
						|| field.fieldName == GDSField.GDS_VERSION_FIELD
						|| field.fieldName == GDSClass.GDS_CLASS_FIELD
						|| field.fieldName == GDSClass.GDS_FILTERCLASS_FIELD
						|| field.fieldName == GDSClass.GDS_MAP_FIELD
						|| field.nonDatastoreObject)
					continue;

				Map<String, Object> inner = new HashMap<String, Object>();
				if (!field.isAnalyzed) {
					inner.put("index", "not_analyzed");
				}
				
				if (field.es_mapping != null) {
					inner.put("type", field.es_mapping);
				} else {
					inner.put("type", "string");
				}
				
				if (!inner.isEmpty())
					mapping.put(field.fieldName, inner);
			}
			
			Map<String, Object> pmap = new HashMap<String, Object>();
			pmap.put("properties", mapping);
			return pmap;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
