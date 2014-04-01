package com.rc.gds;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.rc.gds.interfaces.GDSMultiResult;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.GDSResultListReceiver;
import com.rc.gds.interfaces.GDSResultReceiver;

public class GDSQueryResultImpl<T> implements GDSMultiResult<T> {
	
	private static final int MAX_DEPTH = 100;
	
	GDSImpl gds;
	Class<T> clazz;
	GDSLoaderImpl loader;
	private boolean finished = false;
	int depth = 0;
	ExecutorService deepStackExecutor;
	GDSResult<SearchResponse> sr;
	
	protected GDSQueryResultImpl(GDSImpl gds, Class<T> clazz, GDSResult<SearchResponse> sr) {
		this.gds = gds;
		this.clazz = clazz;
		this.sr = sr;
		loader = new GDSLoaderImpl(gds);
	}
	
	@Override
	public void later(final GDSResultReceiver<T> resultReceiver) {
		sr.later((searchResponse, err) -> {
			if (err != null) {
				resultReceiver.onError(err);
			} else {
				doFetch(resultReceiver, searchResponse.getHits().iterator());
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	private void doFetch(final GDSResultReceiver<T> resultReceiver, final Iterator<SearchHit> iterator) {
		Entity entity = null;
		SearchHit hit = null;
		boolean notfinished;
		
		synchronized (iterator) {
			notfinished = iterator.hasNext();
			if (notfinished) {
				hit = iterator.next();
				entity = new Entity(hit.getType(), hit.getId(), hit.getSource());
			}
		}
		
		if (!notfinished) {
			resultReceiver.finished();
			return;
		}
		
		final List<GDSLink> links = new ArrayList<GDSLink>();
		try {
			loader.entityToPOJO(entity, hit.getId(), links).later((pojo, err1) -> {
				try {
					if (err1 != null)
						throw err1;
					
					loader.fetchLinks(links).later((t, err2) -> {
						if (err2 != null) {
							err2.printStackTrace();
							shutdownDeepStackExecutor();
							resultReceiver.onError(err2);
						} else {
							if (resultReceiver.receiveNext((T) pojo)) {
								sendNext(resultReceiver, iterator);
							} else {
								shutdownDeepStackExecutor();
								resultReceiver.finished();
							}
						}
					});
				} catch (Throwable e) {
					//resultReceiver.onError(e);
					if (resultReceiver.receiveNext(null)) {
						sendNext(resultReceiver, iterator);
					} else {
						shutdownDeepStackExecutor();
						resultReceiver.finished();
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error completing query on " + clazz, e);
		}
	}
	
	@Override
	public List<T> asList() {
		finished = false;
		final List<T> result = new ArrayList<>();
		final List<Throwable> errList = new ArrayList<>();
		
		later(new GDSResultReceiver<T>() {
			
			@Override
			public boolean receiveNext(T t) {
				synchronized (result) {
					result.add(t);
				}
				return true;
			}
			
			@Override
			public void finished() {
				synchronized (result) {
					finished = true;
					result.notifyAll();
				}
			}
			
			@Override
			public void onError(Throwable err) {
				synchronized (result) {
					if (!errList.isEmpty())
						return;
					errList.add(new RuntimeException(err));
					result.notifyAll();
				}
			}
		});
		
		synchronized (result) {
			try {
				if (!errList.isEmpty())
					throw new RuntimeException("Error completing query on " + clazz, errList.get(0));
				if (finished)
					return result;
				result.wait();
				if (!errList.isEmpty())
					throw new RuntimeException("Error completing query on " + clazz, errList.get(0));
				return result;
			} catch (InterruptedException e) {
				throw new RuntimeException("Error completing query on " + clazz, e);
			}
		}
	}
	
	@Override
	public void laterAsList(final GDSResultListReceiver<T> resultListReceiver) {
		later(new GDSResultReceiver<T>() {
			
			List<T> list = new ArrayList<>();
			
			@Override
			public boolean receiveNext(T t) {
				list.add(t);
				return true;
			}
			
			@Override
			public void finished() {
				resultListReceiver.success(list, null);
			}
			
			@Override
			public void onError(Throwable err) {
				resultListReceiver.success(null, err);
			}
		});
	}
	
	private void shutdownDeepStackExecutor() {
		if (deepStackExecutor != null)
			deepStackExecutor.shutdown();
	}
	
	private void sendNext(final GDSResultReceiver<T> resultReceiver, final Iterator<SearchHit> iterator) {
		depth++;
		if (depth > MAX_DEPTH) {
			depth = 0;
			// Temporary work around for depth getting too great on the stack
			if (deepStackExecutor == null)
				deepStackExecutor = Executors.newSingleThreadExecutor();
			deepStackExecutor.submit(() -> doFetch(resultReceiver, iterator));
		} else {
			doFetch(resultReceiver, iterator);
		}
	}
	
}
