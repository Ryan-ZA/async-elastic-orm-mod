package com.rc.gds;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSLoader;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.Key;

public class GDSLoaderImpl implements GDSLoader {

	GDSImpl gds;
	private Map<Key, Object> localCache = Collections.synchronizedMap(new HashMap<Key, Object>());

	protected GDSLoaderImpl(GDSImpl gds) {
		this.gds = gds;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSLoader#fetch(java.lang.Class, java.lang.String)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> GDSResult<T> fetch(final Class<T> clazz, final String id) {
		try {
			ESMapCreator.ensureIndexCreated(gds, clazz);
			final GDSAsyncImpl<T> callback = new GDSAsyncImpl<>();

			final String kind = GDSClass.getKind(clazz);
			final Key key = new Key(kind, id);

			if (localCache.containsKey(key)) {
				callback.onSuccess((T) localCache.get(key), null);
				return callback;
			}

			gds.getClient().prepareGet(gds.indexFor(key.kind), key.kind, key.id)
					.execute(new ActionListener<GetResponse>() {
						
						@Override
						public void onResponse(GetResponse response) {
							if (!response.isExists()) {
								callback.onSuccess(null, null);
								return;
							}
							Entity entity = new Entity(kind, response.getId(), response.getSourceAsMap());

							final List<GDSLink> linksToFetch = Collections.synchronizedList(new ArrayList<GDSLink>());
							try {
								entityToPOJO(entity, entity.getKey().getId(), linksToFetch).later(new GDSCallback<Object>() {
									
									@Override
									public void onSuccess(final Object pojo, Throwable err) {
										try {
											if (err != null)
												throw err;
											localCache.put(key, pojo);
											fetchLinks(linksToFetch).later(new GDSCallback<List<GDSLink>>() {

												@Override
												public void onSuccess(List<GDSLink> t, Throwable err) {
													callback.onSuccess((T) pojo, err);
												}
											});
										} catch (Throwable e) {
											callback.onSuccess(null, e);
										}
									}
								});
							} catch (Throwable e) {
								callback.onSuccess(null, e);
							}
						}
						
						@Override
						public void onFailure(Throwable e) {
							callback.onSuccess(null, e);
						}
					});

			return callback;
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSLoader#fetch(com.rc.gds.Key, com.rc.gds.interfaces.GDSCallback)
	 */
	@Override
	public GDSResult<Object> fetch(final Key key) {
		if (localCache.containsKey(key))
			return new GDSAsyncImpl<Object>(localCache.get(key));
		
		final GDSAsyncImpl<Object> result = new GDSAsyncImpl<>();
		
		gds.getClient().prepareGet(gds.indexFor(key.kind), key.kind, key.id)
				.execute(new ActionListener<GetResponse>() {
					
					@Override
					public void onResponse(GetResponse getResponse) {
						try {
							Entity entity = new Entity(key.kind, getResponse.getId(), getResponse.getSourceAsMap());
							final List<GDSLink> linksToFetch = Collections.synchronizedList(new ArrayList<GDSLink>());
							entityToPOJO(entity, entity.getKey().getId(), linksToFetch).later(new GDSCallback<Object>() {
								
								@Override
								public void onSuccess(final Object pojo, Throwable err) {
									try {
										if (err != null)
											throw err;
										localCache.put(key, pojo);
										fetchLinks(linksToFetch).later(new GDSCallback<List<GDSLink>>() {
											
											@Override
											public void onSuccess(List<GDSLink> t, Throwable err) {
												result.onSuccess(pojo, err);
											}
										});
									} catch (Throwable e) {
										result.onSuccess(null, e);
									}
								}
							});
						} catch (Exception e) {
							result.onSuccess(null, e);
						}
					}
					
					@Override
					public void onFailure(Throwable e) {
						if (e instanceof EsRejectedExecutionException) {
							try {
								System.out.println("Retrying...");
								Thread.sleep(50);
								gds.getClient().prepareGet(gds.indexFor(key.kind), key.kind, key.id).execute(this);
							} catch (InterruptedException e1) {
							}
						} else {
							result.onSuccess(null, e);
						}
					}
				});
		
		return result;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSLoader#fetchBatch(java.lang.Iterable, com.rc.gds.interfaces.GDSCallback)
	 */
	@Override
	public GDSResult<Map<Key, Object>> fetchBatch(final Iterable<Key> keys) throws InterruptedException, ExecutionException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		final Map<Key, Object> fetched = Collections.synchronizedMap(new HashMap<Key, Object>());
		final Set<Key> stillToFetch = Collections.synchronizedSet(new HashSet<Key>());
		
		for (Key key : keys) {
			Object pojo = localCache.get(key);
			if (pojo == null)
				stillToFetch.add(key);
			else
				fetched.put(key, pojo);
		}
		
		if (stillToFetch.isEmpty())
			return new GDSAsyncImpl<Map<Key, Object>>(fetched);
		
		final List<GDSLink> linksToFetch = Collections.synchronizedList(new ArrayList<GDSLink>());
		final GDSAsyncImpl<Map<Key, Object>> realResult = new GDSAsyncImpl<>();
		bulkFetchList(stillToFetch).later(new GDSCallback<Map<Key, Entity>>() {
			
			@Override
			public void onSuccess(Map<Key, Entity> dsMap, Throwable err) {
				try {
					if (err != null)
						throw err;
					final Map<Key, GDSResult<?>> objResults = new HashMap<>();
					for (final Entry<Key, Entity> entry : dsMap.entrySet()) {
						Entity entity = entry.getValue();
						final Key key = entry.getKey();
						
						objResults.put(key, entityToPOJO(entity, key.getId(), linksToFetch));
					}
					new GDSBatcher(objResults.values()).onAllComplete().later(new GDSCallback<Boolean>() {
						
						@Override
						public void onSuccess(Boolean t, Throwable err) {
							if (err != null)
								realResult.onSuccess(null, err);
							
							try {
								final Map<Key, Object> objectMap = new HashMap<>();
								for (Entry<Key, GDSResult<?>> entry : objResults.entrySet()) {
									objectMap.put(entry.getKey(), entry.getValue().now());
								}
								localCache.putAll(objectMap);
								fetchLinks(linksToFetch).later(new GDSCallback<List<GDSLink>>() {
									
									@Override
									public void onSuccess(List<GDSLink> t, Throwable err) {
										realResult.onSuccess(objectMap, err);
									}
								});
							} catch (Exception e) {
								realResult.onSuccess(null, e);
							}
						}
					});
				} catch (Throwable e) {
					realResult.onSuccess(null, e);
				}
			}
		});
		return realResult;
	}

	private GDSResult<Map<Key, Entity>> bulkFetchList(Set<Key> stillToFetch) {
		
		final GDSAsyncImpl<Map<Key, Entity>> result = new GDSAsyncImpl<>();

		MultiGetRequestBuilder requestBuilder = gds.getClient().prepareMultiGet();

		for (Key key : stillToFetch) {
			requestBuilder.add(gds.indexFor(key.kind), key.kind, key.id);
		}
		
		requestBuilder.execute(new ActionListener<MultiGetResponse>() {
			
			@Override
			public void onResponse(MultiGetResponse response) {
				Map<Key, Entity> resultMap = Collections.synchronizedMap(new HashMap<Key, Entity>());
				for (MultiGetItemResponse itemResponse : response.getResponses()) {
					Entity entity = new Entity(itemResponse.getType(), itemResponse.getResponse().getId(), itemResponse.getResponse().getSourceAsMap());
					resultMap.put(new Key(itemResponse.getType(), itemResponse.getId()), entity);
					if (entity.dbObject == null) {
						result.onSuccess(null, new Exception("Item does not exist: " + itemResponse.getType() + " " + itemResponse.getId()));
						return;
					}
				}
				result.onSuccess(resultMap, null);
			}
			
			@Override
			public void onFailure(Throwable e) {
				result.onSuccess(null, e);
			}
		});
		
		return result;
	}
	
	public GDSResult<List<GDSLink>> fetchLinks(final List<GDSLink> linksToFetch) throws Exception {

		Set<Key> stillToFetch = Collections.synchronizedSet(new HashSet<Key>());

		for (GDSLink link : linksToFetch) {
			if (link.keyCollection != null)
				stillToFetch.addAll(link.keyCollection);
			if (link.key != null)
				stillToFetch.add(link.key);
		}
		
		if (stillToFetch.isEmpty())
			return new GDSAsyncImpl<List<GDSLink>>(linksToFetch);
		
		final GDSAsyncImpl<List<GDSLink>> result = new GDSAsyncImpl<>();
		fetchBatch(stillToFetch).later(new GDSCallback<Map<Key, Object>>() {
			
			@Override
			public void onSuccess(Map<Key, Object> fetched, Throwable err) {
				try {
					if (err != null)
						throw err;

					for (GDSLink link : linksToFetch) {
						if (link.key != null) {
							Object newPojo = fetched.get(link.key);
							if (newPojo == null)
								continue;
							
							if (link.collection != null) {
								link.collection.add(newPojo);
							} else if (link.gdsField != null && link.pojo != null) {
								link.gdsField.field.set(link.pojo, newPojo);
							} else {
								throw new RuntimeException("Undefined behaviour");
							}
						}
						if (link.keyCollection != null) {
							if (link.collection != null) {
								for (Key key : link.keyCollection) {
									link.collection.add(fetched.get(key));
								}
							} else if (link.gdsField != null && link.gdsField.isArray) {
								Class<?> fieldType = link.gdsField.field.getType().getComponentType();
								Object newArray = Array.newInstance(fieldType, link.keyCollection.size());
								int counter = 0;
								for (Key key : link.keyCollection) {
									Object childPOJO = fetched.get(key);
									setArrayIndex(counter, fieldType, newArray, childPOJO);
									counter++;
								}
							} else {
								throw new RuntimeException("Undefined behaviour");
							}
						}
					}
					result.onSuccess(linksToFetch, err);
				} catch (Throwable e) {
					result.onSuccess(linksToFetch, e);
				}
			}
		});
		return result;
	}
	
	/**
	 * Real logic for loading pojos from the datastore. Can be given a Entity or EmbeddedEntity, and if the entity is in the correct format
	 * you will get back a POJO.
	 * 
	 * @param entity
	 * @param id
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public GDSResult<Object> entityToPOJO(PropertyContainer entity, String id, final List<GDSLink> linksToFetch) throws ClassNotFoundException,
			InstantiationException,
			IllegalAccessException,
			InterruptedException,
			ExecutionException {
		
		final List<GDSResult<?>> results = new ArrayList<>();

		String kind = (String) entity.getProperty(GDSClass.GDS_CLASS_FIELD);

		Class<?> clazz = Class.forName(kind.replace("_", ".").replace("##", "_"));
		GDSClass.makeConstructorsPublic(clazz);
		final Object pojo = clazz.newInstance();
		Map<String, GDSField> map = GDSField.createMapFromObject(gds, pojo);
		
		final List<GDSCallback<Void>> asyncWorkList = Collections.synchronizedList(new ArrayList<GDSCallback<Void>>());
		GDSCallback<Void> block = new GDSCallback<Void>() {
			
			@Override
			public void onSuccess(Void t, Throwable e) {
				// Never called
			}
		};
		asyncWorkList.add(block);

		for (final GDSField gdsField : map.values()) {
			if (gdsField.fieldName.equals(GDSField.GDS_ID_FIELD)) {
				// ID field is part of the key of the entity
				gdsField.field.set(pojo, id);
				continue;
			}

			if (entity.getProperty(gdsField.fieldName) == null) {
				// No value for this field was saved, leave it blank
				continue;
			}

			Class<?> fieldType = gdsField.field.getType();

			if (fieldType.isArray()) {
				fieldType = fieldType.getComponentType();
				// Field is an array - we need to take the list from GAE,
				// get the number of elements, create an array of that size,
				// then set each element to the correct value

				List<Object> dsCollection = (List<Object>) entity.getProperty(gdsField.fieldName);
				final Object newArray = Array.newInstance(fieldType, dsCollection.size());

				if (gdsField.nonDatastoreObject) {
					if (gdsField.embedded) {
						// In this case, all stored objects will be
						// EmbeddedEntities that we need to turn into POJOs
						results.add(fillArrayAsync(entity, linksToFetch, gdsField, fieldType, newArray));
					} else {
						// In this case, all stored objects in dsCollection will
						// be keys that we need to fetch and turn into actual
						// POJOs.
						Collection<Map<String, Object>> mapCollection = (Collection<Map<String, Object>>) entity.getProperty(gdsField.fieldName);
						Collection<Key> keyCollection = new ArrayList<Key>();
						for (Map<String, Object> m : mapCollection) {
							keyCollection.add(new Key(m));
						}
						linksToFetch.add(new GDSLink(pojo, gdsField, keyCollection));
					}
				} else {
					for (int i = 0; i < dsCollection.size(); i++) {
						setArrayIndex(i, fieldType, newArray, dsCollection.get(i));
					}
				}
				setField(gdsField.field, pojo, newArray);
			} else if (Collection.class.isAssignableFrom(fieldType)) {
				// Field is some kind of collection - so we need to take the
				// collection we get from GAE and convert it into the field's
				// subclass of collection
				final Collection<Object> newCollection = (Collection<Object>) GDSBoxer.createBestFitCollection(fieldType);

				if (gdsField.nonDatastoreObject) {
					results.add(fillCollectionAsync(entity, linksToFetch, gdsField, newCollection));
				} else {
					Collection<Object> dsCollection = (Collection<Object>) entity.getProperty(gdsField.fieldName);

					for (Object object : dsCollection) {
						newCollection.add(object);
					}
				}
				setField(gdsField.field, pojo, newCollection);
			} else if (Map.class.isAssignableFrom(fieldType)) {
				// Field is some kind of map - so we need to take the
				// collection we get from GAE and convert it into a map using
				// special K and V properties.
				final Map<Object, Object> newMap = (Map<Object, Object>) GDSBoxer.createBestFitMap(fieldType);
				
				results.add(fillMapAsync(entity, linksToFetch, gdsField, newMap));
				
				setField(gdsField.field, pojo, newMap);
			} else if (gdsField.nonDatastoreObject) {
				if (gdsField.embedded) {
					Map<String, Object> dbObject = (Map<String, Object>) entity.getProperty(gdsField.fieldName);
					EmbeddedEntity embeddedEntity = new EmbeddedEntity();
					embeddedEntity.dbObject = dbObject;
					
					GDSResult<Object> result = entityToPOJO(embeddedEntity, null, linksToFetch);
					result.later(new GDSCallback<Object>() {
						
						@Override
						public void onSuccess(Object embeddedPOJO, Throwable err) {
							synchronized (pojo) {
								try {
									setField(gdsField.field, pojo, embeddedPOJO);
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							}
						}
					});
					results.add(result);
				} else {
					// Get the datastore key, we will use it later to fetch all
					// children at once
					Key key = new Key((Map<String, Object>) entity.getProperty(gdsField.fieldName));
					linksToFetch.add(new GDSLink(pojo, gdsField, key));
				}
			} else if (gdsField.isEnum) {
				String enumName = entity.getProperty(gdsField.fieldName).toString();
				setField(gdsField.field, pojo, Enum.valueOf((Class<? extends Enum>) gdsField.field.getType(), enumName));
			} else {
				// Regular/primitive fields
				Object fieldPOJO = entity.getProperty(gdsField.fieldName);
				setField(gdsField.field, pojo, fieldPOJO);
			}
		}

		final GDSAsyncImpl<Object> realResult = new GDSAsyncImpl<Object>();
		new GDSBatcher(results).onAllComplete().later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				realResult.onSuccess(pojo, err);
			}
		});
		return realResult;
	}

	@SuppressWarnings("unchecked")
	private GDSResult<Boolean> fillMapAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, final Map<Object, Object> newMap)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {

		EmbeddedEntity embeddedEntity = new EmbeddedEntity();
		embeddedEntity.dbObject = (Map<String, Object>) entity.getProperty(gdsField.fieldName);

		final List<GDSResult<?>> allResults = new ArrayList<>();
		final List<GDSResult<?>> keyResults = new ArrayList<>();
		final List<GDSResult<?>> valResults = new ArrayList<>();
		
		for (int counter = 1;; counter++) {
			if (!embeddedEntity.hasProperty("K" + counter))
				break;
			
			final Object keyFind = embeddedEntity.getProperty("K" + counter);
			final Object valFind = embeddedEntity.getProperty("V" + counter);
			
			GDSResult<Object> result;
			
			result = mapValueToPOJO(keyFind, linksToFetch);
			allResults.add(result);
			keyResults.add(result);
			
			result = mapValueToPOJO(valFind, linksToFetch);
			allResults.add(result);
			valResults.add(result);
		}
		
		GDSResult<Boolean> onComplete = new GDSBatcher(allResults).onAllComplete();
		onComplete.later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				for (int i = 0; i < keyResults.size(); i++) {
					Object key = keyResults.get(i).now();
					Object val = valResults.get(i).now();
					newMap.put(key, val);
				}
			}
		});
		return onComplete;

	}
	
	@SuppressWarnings("unchecked")
	private GDSResult<Boolean> fillCollectionAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, final Collection<Object> newCollection)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		
		Collection<Object> dsCollection = (Collection<Object>) entity.getProperty(gdsField.fieldName);
		final List<GDSResult<?>> results = new ArrayList<>();

		for (final Object obj : dsCollection) {
			if (obj instanceof Key) {
				Key key = (Key) obj;
				linksToFetch.add(new GDSLink(newCollection, key));
			} else {
				results.add(mapValueToPOJO(obj, linksToFetch));
			}
		}
		
		GDSResult<Boolean> onComplete = new GDSBatcher(results).onAllComplete();
		onComplete.later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				for (GDSResult<?> r : results) {
					newCollection.add(r.now());
				}
			}
		});
		return onComplete;
	}
	
	@SuppressWarnings("unchecked")
	private GDSResult<Boolean> fillArrayAsync(PropertyContainer entity, final List<GDSLink> linksToFetch, final GDSField gdsField, final Class<?> fieldType, final Object newArray)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {

		Collection<HashMap<String, Object>> collection = (Collection<HashMap<String, Object>>) entity.getProperty(gdsField.fieldName);
		final List<GDSResult<?>> results = new ArrayList<>();

		for (final HashMap<String, Object> embeddedEntity : collection) {
			EmbeddedEntity tmp = new EmbeddedEntity();
			tmp.dbObject = embeddedEntity;
			results.add(entityToPOJO(tmp, null, linksToFetch));
		}
		GDSResult<Boolean> onComplete = new GDSBatcher(results).onAllComplete();
		onComplete.later(new GDSCallback<Boolean>() {
			
			@Override
			public void onSuccess(Boolean t, Throwable err) {
				for (int i = 0; i < results.size(); i++) {
					setArrayIndex(i, fieldType, newArray, results.get(i).now());
				}
			}
		});
		return onComplete;
	}
	
	@SuppressWarnings("unchecked")
	private GDSResult<Object> mapValueToPOJO(Object obj, List<GDSLink> linksToFetch) throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, InterruptedException,
			ExecutionException {
		
		if (obj instanceof Map<?, ?>) {
			Map<String, Object> map = (Map<String, Object>) obj;
			if (!map.containsKey(GDSClass.GDS_CLASS_FIELD)) {
				Key key = new Key(map);
				return fetch(key);
			} else {
				EmbeddedEntity entity = new EmbeddedEntity();
				entity.dbObject = map;
				return entityToPOJO(entity, null, linksToFetch);
			}
		} else {
			return new GDSAsyncImpl<>(obj);
		}
	}

	/**
	 * Nasty function to get around nasty java primitive arrays.
	 * 
	 * @param index
	 * @param type
	 * @param array
	 * @param fieldPOJO
	 */
	private void setArrayIndex(int index, Class<?> type, Object array, Object fieldPOJO) {
		if (type.isPrimitive()) {
			if (type == int.class) {
				Array.setInt(array, index, (Integer) fieldPOJO);
			} else if (type == short.class) {
				Integer integer = (Integer) fieldPOJO;
				Array.setShort(array, index, integer.shortValue());
			} else if (type == byte.class) {
				Array.setByte(array, index, Byte.parseByte(fieldPOJO.toString()));
			} else if (type == float.class) {
				Double d = (Double) fieldPOJO;
				Array.setFloat(array, index, d.floatValue());
			} else if (type == char.class) {
				String s = (String) fieldPOJO;
				Array.setChar(array, index, s.charAt(0));
			} else {
				Array.set(array, index, fieldPOJO);
			}
		} else {
			if (type == Integer.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.intValue());
			} else if (type == Short.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.shortValue());
			} else if (type == Byte.class) {
				Long l = (Long) fieldPOJO;
				Array.set(array, index, l.byteValue());
			} else if (type == Float.class) {
				Double d = (Double) fieldPOJO;
				Array.set(array, index, d.floatValue());
			} else if (type == Character.class) {
				String s = (String) fieldPOJO;
				fieldPOJO = new Character(s.charAt(0));
				Array.set(array, index, fieldPOJO);
			} else {
				System.out.println("array: " + array);
				System.out.println("type: " + type);
				System.out.println("fieldPOJO: " + fieldPOJO);
				if (fieldPOJO instanceof Object[]) {
					System.out.println("fieldPOJOc: " + fieldPOJO.getClass());
					Object[] a = (Object[]) fieldPOJO;
					System.out.println("fieldPOJOin: " + a[0] + " ::: " + a[0].getClass());
				}
				Array.set(array, index, fieldPOJO);
			}
		}
	}

	/**
	 * Nasty function to get around nasty java primitive fields.
	 * 
	 * @param field
	 * @param pojo
	 * @param fieldPOJO
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	private void setField(Field field, Object pojo, Object fieldPOJO) throws IllegalArgumentException, IllegalAccessException {
		Class<?> type = field.getType();
		if (type.isPrimitive()) {
			if (type == int.class) {
				Integer l = (Integer) fieldPOJO;
				field.setInt(pojo, l);
			} else if (type == short.class) {
				Integer l = (Integer) fieldPOJO;
				field.setShort(pojo, l.shortValue());
			} else if (type == byte.class) {
				String l = (String) fieldPOJO;
				field.setByte(pojo, Byte.valueOf(l));
			} else if (type == float.class) {
				Double d = (Double) fieldPOJO;
				field.setFloat(pojo, d.floatValue());
			} else if (type == char.class) {
				Character s = (Character) fieldPOJO;
				field.setChar(pojo, s);
			} else {
				field.set(pojo, fieldPOJO);
			}
		} else {
			if (Number.class.isAssignableFrom(type)) {
				Number number;
				if (fieldPOJO instanceof String) {
					number = Double.valueOf((String) fieldPOJO);
				} else {
					number = (Number) fieldPOJO;
				}
				if (type == Integer.class) {
					field.set(pojo, number.intValue());
				} else if (type == Long.class) {
					field.set(pojo, number.longValue());
				} else if (type == Short.class) {
					field.set(pojo, number.shortValue());
				} else if (type == Byte.class) {
					field.set(pojo, number.byteValue());
				} else if (type == Float.class) {
					field.set(pojo, number.floatValue());
				} else if (type == Double.class) {
					field.set(pojo, number.doubleValue());
				} else {
					field.set(pojo, number.doubleValue());
				}
			} else if (type == Character.class) {
				Character s = (Character) fieldPOJO;
				field.set(pojo, s);
			} else {
				field.set(pojo, fieldPOJO);
			}
		}
	}

}
