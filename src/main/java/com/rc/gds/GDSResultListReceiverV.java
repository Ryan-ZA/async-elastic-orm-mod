package com.rc.gds;

import java.util.List;

import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

import com.rc.gds.interfaces.GDSResultListReceiver;

public abstract class GDSResultListReceiverV<T> implements GDSResultListReceiver<T> {
	
	private Context context;
	
	public GDSResultListReceiverV(Vertx vertx) {
		this.context = vertx.currentContext();
	}
	
	public abstract void onSuccessV(List<T> list, Throwable err);
	
	@Override
	public void success(final List<T> list, final Throwable err) {
		context.runOnContext(new Handler<Void>() {
			
			@Override
			public void handle(Void event) {
				onSuccessV(list, err);
			}
		});
	}
	
}
