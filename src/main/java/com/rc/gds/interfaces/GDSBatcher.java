package com.rc.gds.interfaces;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import com.rc.gds.GDSAsyncImpl;

public class GDSBatcher {
	
	final GDSResult<?>[] singleResults;
	final GDSMultiResult<?>[] multiResults;
	
	public GDSBatcher(Collection<GDSResult<?>> input) {
		singleResults = input.toArray(new GDSResult[0]);
		multiResults = null;
	}
	
	public GDSBatcher(GDSResult<?>... input) {
		singleResults = input;
		multiResults = null;
	}
	
	public GDSBatcher(GDSMultiResult<?>... input) {
		singleResults = null;
		multiResults = input;
	}
	
	public GDSBatcher(GDSResult<?>[] singleResults, GDSMultiResult<?>[] multiResults) {
		this.singleResults = singleResults;
		this.multiResults = multiResults;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public GDSResult<Boolean> onAllComplete() {
		final AtomicInteger current = new AtomicInteger(0);
		final AtomicInteger total = new AtomicInteger(0);
		final GDSAsyncImpl<Boolean> asyncResult = new GDSAsyncImpl<>();
		
		final GDSCallback singleCallback = (t, err) -> handleSuccess(current, total, asyncResult, err);
		final GDSResultListReceiver multiCallback = (list, err) -> handleSuccess(current, total, asyncResult, err);
		
		if (singleResults != null) {
			total.addAndGet(singleResults.length);
			for (GDSResult<?> result : singleResults)
				result.later(singleCallback);
		}
		if (multiResults != null) {
			total.addAndGet(multiResults.length);
			for (GDSMultiResult<?> result : multiResults)
				result.laterAsList(multiCallback);
		}
		
		if (total.get() == 0)
			asyncResult.onSuccess(true, null);
		
		return asyncResult;
	}
	
	protected void handleSuccess(AtomicInteger current, AtomicInteger total, GDSAsyncImpl<Boolean> result, Throwable err) {
		if (err != null) {
			result.onSuccess(false, err);
		} else if (current.incrementAndGet() == total.get()) {
			result.onSuccess(true, err);
		}
	}
	
}
