package com.rc.gds;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import com.rc.gds.annotation.AlwaysPersist;
import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.GDSSaver;
import com.rc.gds.interfaces.Key;

public class GDSSaverImpl implements GDSSaver {
	
	GDSImpl gds;
	boolean recursiveUpdate;
	List<Object> alreadyStoredObjects;
	int retrycount = 0;
	
	protected GDSSaverImpl(GDSImpl gds) {
		this.gds = gds;
		alreadyStoredObjects = Collections.synchronizedList(new ArrayList<Object>());
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#forceRecursiveUpdate(boolean)
	 */
	@Override
	public GDSSaver forceRecursiveUpdate(boolean recursiveUpdate) {
		this.recursiveUpdate = recursiveUpdate;
		return this;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private GDSResult<Entity> createEntity(final Object pojo, final boolean isEmbedded) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		final GDSAsyncImpl result = new GDSAsyncImpl<>();
		result.runOnStart = new Runnable() {
			
			@Override
			public void run() {
				try {
					GDSResult<?> preSaveResult = GDSClass.onPreSave(gds, pojo);
					if (preSaveResult == null) {
						createEntityInt(pojo, isEmbedded).later(result);
					} else {
						preSaveResult.later(new GDSCallback() {
							
							@Override
							public void onSuccess(Object t, Throwable err) {
								try {
									createEntityInt(pojo, isEmbedded).later(result);
								} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
									result.onSuccess(null, e);
								}
							}
						});
					}
					
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					result.onSuccess(null, e);
				}
			}
		};
		return ESMapCreator.ensureIndexCreated(gds, pojo.getClass(), result);
	}
	
	private GDSResult<Entity> createEntityInt(Object pojo, boolean isEmbedded) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Map<String, GDSField> fieldMap = GDSField.createMapFromObject(pojo);
		
		String id = null;
		if (!isEmbedded) {
			GDSField idfield = fieldMap.get(GDSField.GDS_ID_FIELD);
			if (idfield == null)
				throw new RuntimeException("Class " + pojo.getClass().getName() + " does not have an ID field!");
			
			// Get the ID and create the low level entity
			id = (String) GDSField.getValue(idfield, pojo);
		}
		
		List<String> classKinds = GDSClass.getKinds(pojo.getClass());
		// classKind is the top most superclass for the pojo. All subclasses
		// will have the same kind to allow for querying across subclasses.
		String classKind = classKinds.get(classKinds.size() - 1);
		
		final Entity entity;
		if (id == null) {
			entity = new Entity(classKind);
		} else {
			entity = new Entity(classKind, id);
		}
		
		// If the object is versioned we set it here
		GDSField verfield = fieldMap.get(GDSField.GDS_VERSION_FIELD);
		if (!isEmbedded && verfield != null)
			entity.setVersion(verfield.field.getLong(pojo));
		
		// Add indexed class and superclass information for easy polymorphic
		// querying
		if (!isEmbedded)
			entity.setProperty(GDSClass.GDS_FILTERCLASS_FIELD, classKinds);
		entity.setProperty(GDSClass.GDS_CLASS_FIELD, classKinds.get(0));
		
		final Map<GDSField, GDSResult<?>> fieldResults = new HashMap<>();
		
		// Add the field values
		for (GDSField field : fieldMap.values()) {
			GDSResult<Object> objResult = addFieldToEntity(pojo, field, entity);
			if (objResult != null)
				fieldResults.put(field, objResult);
		}
		
		final GDSAsyncImpl<Entity> result = new GDSAsyncImpl<>();
		new GDSBatcher(fieldResults.values()).onAllComplete().later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				for (Entry<GDSField, GDSResult<?>> entry : fieldResults.entrySet())
					setEntityProperty(entity, entry.getKey(), entry.getValue().now());
				
				result.onSuccess(entity, err);
			}
		});
		return result;
	}
	
	/**
	 * 
	 * @return NULL is returned if the method was able to add the object to the entity synchronously to avoid the overhead of creating
	 *         result objects
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private GDSResult<Object> addFieldToEntity(Object pojo, GDSField field, PropertyContainer entity) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		if (field.fieldName.equals(GDSField.GDS_ID_FIELD))
			return null;
		
		Object fieldValue = GDSField.getValue(field, pojo);
		
		if (fieldValue == null)
			return null;
		
		if (field.isArray) {
			// This will convert the array into a Collection while also boxing
			// all primitives to allow for insertion in a Collection.
			// GAE cannot deal with arrays - only with collections.
			fieldValue = GDSBoxer.boxArray(fieldValue);
		}
		
		GDSAsyncImpl<Object> result = null;
		
		if (field.isEnum) {
			Enum<?> e = (Enum<?>) fieldValue;
			setEntityProperty(entity, field, e.name());
		} else if (!field.nonDatastoreObject) {
			setEntityProperty(entity, field, fieldValue);
		} else if (fieldValue instanceof Collection<?>) {
			result = (GDSAsyncImpl) storeCollectionOfPOJO(field, (Collection<?>) fieldValue);
		} else if (fieldValue instanceof Map<?, ?>) {
			result = (GDSAsyncImpl) storeMapOfPOJO(field, (Map<?, ?>) fieldValue);
		} else if (field.embedded) {
			result = (GDSAsyncImpl) createEntity(fieldValue, true);
		} else {
			result = (GDSAsyncImpl) createKeyForRegularPOJO(fieldValue);
		}
		
		return result;
	}
	
	private GDSResult<Collection<?>> storeCollectionOfPOJO(GDSField field, Collection<?> fieldValue) throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		
		final List<GDSResult<?>> resultList = new ArrayList<>();
		
		for (Object object : fieldValue) {
			if (object == null) {
				continue;
			} else if (GDSField.nonDSClasses.contains(object.getClass())) {
				resultList.add(new GDSAsyncImpl<Object>(object));
			} else if (GDSClass.hasIDField(object.getClass())) {
				resultList.add(createKeyForRegularPOJO(object));
			} else {
				resultList.add(createEntity(object, true));
			}
		}
		
		final GDSAsyncImpl<Collection<?>> realResult = new GDSAsyncImpl<>();
		new GDSBatcher(resultList).onAllComplete().later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				Collection<Object> collection = new ArrayList<>();
				for (GDSResult<?> result : resultList)
					collection.add(convert(result.now()));
				realResult.onSuccess(collection, err);
			}
		});
		return realResult;
	}
	
	private Object convert(Object val) {
		if (val instanceof Key) {
			return ((Key) val).toMap();
		} else if (val instanceof Entity) {
			return ((Entity) val).getDBDbObject();
		} else {
			return val;
		}
	}

	private GDSResult<EmbeddedEntity> storeMapOfPOJO(GDSField field, Map<?, ?> fieldValue) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		
		final EmbeddedEntity mapEntity = new EmbeddedEntity();
		
		final List<GDSResult<?>> allResults = new ArrayList<>();
		final List<GDSResult<?>> keyResults = new ArrayList<>();
		final List<GDSResult<?>> valResults = new ArrayList<>();
		
		for (Entry<?, ?> entry : fieldValue.entrySet()) {
			if (entry.getKey() == null || entry.getValue() == null) {
				continue;
			}
			
			Object input = entry.getKey();
			Class<?> inputClazz = input.getClass();
			GDSResult<?> rKey, rVal;
			
			if (GDSField.nonDSClasses.contains(inputClazz)) {
				rKey = new GDSAsyncImpl<Object>(input);
			} else if (GDSClass.hasIDField(inputClazz)) {
				rKey = createKeyForRegularPOJO(input);
			} else {
				rKey = createEntity(input, true);
			}
			
			input = entry.getValue();
			inputClazz = input.getClass();
			
			if (GDSField.nonDSClasses.contains(inputClazz)) {
				rVal = new GDSAsyncImpl<Object>(input);
			} else if (GDSClass.hasIDField(inputClazz)) {
				rVal = createKeyForRegularPOJO(input);
			} else {
				rVal = createEntity(input, true);
			}
			
			allResults.add(rKey);
			allResults.add(rVal);
			keyResults.add(rKey);
			valResults.add(rVal);
		}
		
		final GDSAsyncImpl<EmbeddedEntity> realResult = new GDSAsyncImpl<>();
		
		new GDSBatcher(allResults).onAllComplete().later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				for (int i = 0; i < keyResults.size(); i++) {
					int ind = i + 1;
					mapEntity.setProperty("K" + ind, convert(keyResults.get(i).now()));
					mapEntity.setProperty("V" + ind, convert(valResults.get(i).now()));
				}
				realResult.onSuccess(mapEntity, err);
			}
		});
		return realResult;
	}
	
	private GDSResult<Key> createKeyForRegularPOJO(final Object fieldValue) throws IllegalArgumentException, IllegalAccessException {
		final String fieldValueKind = GDSClass.getKind(fieldValue);
		Map<String, GDSField> map = GDSField.createMapFromObject(fieldValue);
		final GDSField idfield = map.get(GDSField.GDS_ID_FIELD);
		Object idFieldValue = GDSField.getValue(idfield, fieldValue);
		String id = idfield == null ? null : (String) idFieldValue;
		
		if (recursiveUpdate || id == null || fieldValue.getClass().isAnnotationPresent(AlwaysPersist.class)) {
			if (alreadyStoredObjects.contains(fieldValue)) {
				// We've already saved this object from this call, no need to save it again
				return new GDSAsyncImpl<Key>(new Key(fieldValueKind, id));
			} else {
				alreadyStoredObjects.add(fieldValue);
				boolean isUpdate = true;
				if (id == null) {
					generateIDInPlace(fieldValue, idfield);
					isUpdate = false;
				}
				return result(fieldValue, isUpdate);
			}
		} else {
			return new GDSAsyncImpl<Key>(new Key(fieldValueKind, id));
		}
	}
	
	private void generateIDInPlace(Object fieldValue, GDSField idfield) throws IllegalArgumentException, IllegalAccessException {
		String newid = UUID.randomUUID().toString();
		idfield.field.set(fieldValue, newid);
	}
	
	private void setEntityProperty(PropertyContainer entity, GDSField field, Object value) {
		entity.setProperty(field.fieldName, value);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSSaver#result()
	 */
	@Override
	public GDSResult<Key> result(final Object pojo) {
		return result(pojo, true);
	}
	
	private GDSResult<Key> result(final Object pojo, final boolean isUpdate) {
		final GDSAsyncImpl<Key> result = new GDSAsyncImpl<>();
		
		try {
			createEntity(pojo, false).later(new GDSCallback<Entity>() {
				
				@Override
				public void onSuccess(Entity entity, Throwable err) {
					if (err == null) {
						saveToDatastore(entity, isUpdate).later(new GDSCallback<Key>() {
							
							@SuppressWarnings({ "rawtypes", "unchecked" })
							@Override
							public void onSuccess(final Key key, Throwable err) {
								try {
									if (err != null)
										throw err;
									
									Map<String, GDSField> fieldMap = GDSField.createMapFromObject(pojo);
									GDSField idfield = fieldMap.get(GDSField.GDS_ID_FIELD);
									idfield.field.set(pojo, key.getId());
									GDSField verField = fieldMap.get(GDSField.GDS_VERSION_FIELD);
									if (verField != null)
										verField.field.setLong(pojo, key.version);
									
									GDSResult<?> postSaveResult = GDSClass.onPostSave(gds, pojo);
									if (postSaveResult == null) {
										result.onSuccess(key, null);
									} else {
										postSaveResult.later(new GDSCallback() {
											
											@Override
											public void onSuccess(Object t, Throwable err) {
												result.onSuccess(key, err);
											}
										});
									}
								} catch (Throwable e) {
									result.onSuccess(null, e);
								}
							}
						});
					} else {
						result.onSuccess(null, err);
					}
				}
			});
		} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
			result.onSuccess(null, e);
		}
		
		return result;
	}
	
	private GDSResult<Key> saveToDatastore(final Entity entity, final boolean isUpdate) {
		final GDSAsyncImpl<Key> result = new GDSAsyncImpl<>();
		try {
			// Save to datastore
			if (entity.id != null && isUpdate) {
				UpdateRequestBuilder builder = gds.getClient().prepareUpdate(gds.indexFor(entity.getKind()), entity.getKind(), entity.id);
				builder.setDoc(entity.getDBDbObject());
				Long ver = entity.getVersion();
				if (ver != null)
					builder.setVersion(ver);
				
				builder.execute(new ActionListener<UpdateResponse>() {
					
					@Override
					public void onResponse(UpdateResponse response) {
						Key key = new Key(entity.getKind(), response.getId(), response.getVersion());
						result.onSuccess(key, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						retrycount++;
						if (e instanceof EsRejectedExecutionException && retrycount < 500) {
							try {
								System.out.println("Retrying...");
								Thread.sleep(50);
								saveToDatastore(entity, isUpdate).later(result);
							} catch (InterruptedException e1) {
							}
						} else {
							result.onSuccess(null, e);
						}
					}
				});
			} else {
				IndexRequestBuilder builder = gds.getClient().prepareIndex(gds.indexFor(entity.getKind()), entity.getKind());
				builder.setSource(entity.getDBDbObject());
				if (entity.id != null)
					builder.setId(entity.id);
				builder.execute().addListener(new ActionListener<IndexResponse>() {
					
					@Override
					public void onResponse(IndexResponse indexResponse) {
						Key key = new Key(entity.getKind(), indexResponse.getId(), indexResponse.getVersion());
						result.onSuccess(key, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						retrycount++;
						if (e instanceof EsRejectedExecutionException && retrycount < 500) {
							try {
								System.out.println("Retrying...");
								Thread.sleep(50);
								saveToDatastore(entity, isUpdate).later(result);
							} catch (InterruptedException e1) {
							}
						} else {
							result.onSuccess(null, e);
						}
					}
				});
			}
		} catch (Throwable e) {
			result.onSuccess(null, e);
		}
		return result;
	}
	
}
