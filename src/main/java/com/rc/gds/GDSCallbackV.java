package com.rc.gds;

import org.vertx.java.core.Context;
import org.vertx.java.core.Vertx;

import com.rc.gds.interfaces.GDSCallback;

public abstract class GDSCallbackV<T> implements GDSCallback<T> {
	
	private Context context;
	
	public GDSCallbackV(Vertx vertx) {
		this.context = vertx.currentContext();
	}
	
	@Override
	public final void onSuccess(final T t, final Throwable err) {
		context.runOnContext(event -> onSuccessV(t, err));
	}
	
	public abstract void onSuccessV(T t, Throwable err);
	
}
