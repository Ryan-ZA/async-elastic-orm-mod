package com.rc.gds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.transport.RemoteTransportException;

import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSResult;

class ESMapCreator {
	
	static Set<String> createdKinds = new HashSet<>();
	static Set<String> createdIndexes = new HashSet<>();
	
	/**
	 * Ensure index/mapping created on ES. passResult will be passed through transparently. Does not fix ES merge errors yet...
	 * 
	 * @param gds
	 * @param clazz
	 * @param passResult
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	static GDSResult ensureIndexCreated(final GDSImpl gds, final Class<?> clazz, final GDSResult passResult) {
		List<GDSResult<?>> results = null;
		for (final String kind : GDSClass.getKinds(clazz)) {
			if (createdKinds.add(kind)) {
				
				if (results == null)
					results = new ArrayList<>();
				final GDSAsyncImpl<Boolean> inResult = new GDSAsyncImpl<>();
				results.add(inResult);
				
				final String type = GDSClass.getKind(clazz);
				final String index = gds.indexFor(type);
				gds.getClient().admin().indices().prepareExists(index).execute(new ActionListener<IndicesExistsResponse>() {
					
					@Override
					public void onResponse(IndicesExistsResponse response) {
						if (response.isExists()) {
							gds.getClient().admin().indices().preparePutMapping(index).setType(type).setSource(createMap(gds, clazz))
							.execute(new ActionListener<PutMappingResponse>() {
								
								@Override
								public void onResponse(PutMappingResponse response) {
									inResult.onSuccess(response.isAcknowledged(), null);
								}
								
								@Override
								public void onFailure(Throwable e) {
									inResult.onSuccess(false, e);
								}
							});
						} else {
							gds.getClient().admin().indices().prepareCreate(index).addMapping(type, createMap(gds, clazz))
							.execute(new ActionListener<CreateIndexResponse>() {
								
								@Override
								public void onResponse(CreateIndexResponse response) {
									inResult.onSuccess(response.isAcknowledged(), null);
								}
								
								@Override
								public void onFailure(Throwable e) {
									if (e instanceof RemoteTransportException)
										e = e.getCause();
											
											if (e instanceof IndexAlreadyExistsException) {
										// Already created by some other thread
										inResult.onSuccess(true, null);
									} else {
										inResult.onSuccess(false, e);
									}
								}
							});
						}
					}
					
					@Override
					public void onFailure(Throwable e) {
						inResult.onSuccess(false, e);
					}
				});
			}
		}
		
		if (results == null || passResult == null) {
			return passResult;
		} else {
			final GDSAsyncImpl<?> finalResult = new GDSAsyncImpl<>();
			new GDSBatcher(results).onAllComplete().later((t, err) -> {
				if (err != null)
					finalResult.onSuccess(null, err);
				
				passResult.later(finalResult);
			});
			return finalResult;
		}
	}
	
	private static Map<String, Object> createMap(GDSImpl gds, Class<?> clazz) {
		try {
			Map<String, Object> mapping = new HashMap<String, Object>();
			Map<String, GDSField> gdsMap = GDSField.createMapFromClass(clazz);
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
	
	public static void clearReflection() {
		createdIndexes.clear();
		createdKinds.clear();
	}
	
}
