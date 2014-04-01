package com.rc.gds;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rc.gds.annotation.PostSave;
import com.rc.gds.annotation.PreDelete;
import com.rc.gds.annotation.PreSave;
import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSBatcher;
import com.rc.gds.interfaces.GDSResult;

public class GDSClass {
	
	/**
	 * Includes a list of all superclasses of the class, and the class itself. Used for filtering.
	 */
	public static final String GDS_FILTERCLASS_FIELD = "__GDS_FILTERCLASS_FIELD";
	/**
	 * Java class name for the object. Used to reconstruct the java pojo.
	 */
	public static final String GDS_CLASS_FIELD = "__GDS_CLASS_FIELD";
	
	/**
	 * If this field exists in an Entity, then the field is a Map. Maps are stored as K0=Key1,V0=Value1,K1=Key2,V1=Value2,...
	 */
	public static final String GDS_MAP_FIELD = "__GDS_MAP_FIELD";
	
	static final Map<Class<?>, Boolean> hasIdFieldMap = new ConcurrentHashMap<Class<?>, Boolean>();
	static final Map<Class<?>, List<Method>> hasPreSaveMap = new ConcurrentHashMap<>();
	static final Map<Class<?>, List<Method>> hasPostSaveMap = new ConcurrentHashMap<>();
	static final Map<Class<?>, List<Method>> hasPreDeleteMap = new ConcurrentHashMap<>();
	
	static final Map<Class<?>, Constructor<?>> constructorMap = new ConcurrentHashMap<Class<?>, Constructor<?>>();
	
	public static List<String> getKinds(Class<?> clazz) {
		ArrayList<String> list = new ArrayList<String>();
		while (clazz != null && clazz != Object.class) {
			list.add(fixName(clazz.getName()));
			clazz = clazz.getSuperclass();
		}
		return list;
	}
	
	public static String getKind(Class<?> clazz) {
		return GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());
	}
	
	public static String getKind(Object o) {
		return getKind(o.getClass());
	}
	
	public static String fixName(String classname) {
		return classname.replace("_", "##").replace(".", "_");
	}
	
	public static Class<?> getBaseClass(Class<?> clazz) {
		Class<?> lastclazz = clazz;
		while (clazz != null && clazz != Object.class) {
			lastclazz = clazz;
			clazz = clazz.getSuperclass();
		}
		return lastclazz;
	}
	
	/**
	 * Checks if a class has a usable ID field.
	 * 
	 * @param clazz
	 * @return
	 */
	public static boolean hasIDField(Class<?> clazz) {
		if (hasIdFieldMap.containsKey(clazz)) {
			return hasIdFieldMap.get(clazz);
		}
		
		final Class<?> originalClazz = clazz;
		
		while (clazz != Object.class && clazz != null && !GDSField.nonDSClasses.contains(clazz) && !clazz.isPrimitive() && clazz != Class.class) {
			Field[] classfields = clazz.getDeclaredFields();
			try {
				AccessibleObject.setAccessible(classfields, true);
			} catch (Exception ex) {
				//System.out.println("Error trying to setAccessible for class: " + clazz + " " + ex.toString());
			}
			
			for (Field field : classfields) {
				if (GDSField.createIDField(field) != null) {
					hasIdFieldMap.put(clazz, true);
					return true;
				}
			}
			
			clazz = clazz.getSuperclass();
		}
		hasIdFieldMap.put(originalClazz, false);
		return false;
	}
	
	public static Object construct(Class<?> clazz) {
		try {
			Constructor<?> constructor = constructorMap.get(clazz);
			if (constructor == null) {
				constructor = clazz.getDeclaredConstructor();
				constructor.setAccessible(true);
				constructorMap.put(clazz, constructor);
			}
			return constructor.newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static GDSResult<?> callAnnotatedMethod(GDS gds, Class<? extends Annotation> annotation, Map<Class<?>, List<Method>> annotationMap, Object pojo)
			throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		
		Class<?> clazz = pojo.getClass();
		List<Method> callMethods = annotationMap.get(pojo.getClass());
		if (callMethods == null) {
			callMethods = new ArrayList<>();
			while (clazz != null && clazz != Object.class) {
				for (Method method : pojo.getClass().getDeclaredMethods()) {
					if (method.getAnnotation(annotation) != null) {
						method.setAccessible(true);
						callMethods.add(method);
						break;
					}
				}
				clazz = clazz.getSuperclass();
			}
			
			annotationMap.put(clazz, callMethods);
		}
		
		if (callMethods.isEmpty())
			return null;
		
		if (callMethods.size() == 1)
			return invokeMethod(callMethods.get(0), pojo, gds);
		
		List<GDSResult<?>> results = new ArrayList<>();
		for (Method callMethod : callMethods) {
			GDSResult<?> result = invokeMethod(callMethod, pojo, gds);
			if (result != null)
				results.add(result);
		}
		
		if (results.isEmpty())
			return null;
		
		if (results.size() == 1)
			return results.get(0);
		
		return new GDSBatcher(results).onAllComplete();
	}
	
	private static GDSResult<?> invokeMethod(Method callMethod, Object pojo, GDS gds) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (callMethod.getParameterTypes().length == 0)
			return (GDSResult<?>) callMethod.invoke(pojo);
		else
			return (GDSResult<?>) callMethod.invoke(pojo, gds);
	}
	
	public static GDSResult<?> onPreSave(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return callAnnotatedMethod(gds, PreSave.class, hasPreSaveMap, pojo);
	}
	
	public static GDSResult<?> onPostSave(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return callAnnotatedMethod(gds, PostSave.class, hasPostSaveMap, pojo);
	}
	
	public static GDSResult<?> onPreDelete(GDS gds, Object pojo) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		return callAnnotatedMethod(gds, PreDelete.class, hasPreDeleteMap, pojo);
	}
	
	public static void clearReflection() {
		hasIdFieldMap.clear();
		hasPreSaveMap.clear();
		hasPostSaveMap.clear();
		hasPreDeleteMap.clear();
		constructorMap.clear();
	}
	
}
