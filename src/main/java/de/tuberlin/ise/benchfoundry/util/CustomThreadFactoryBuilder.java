/**
 * 
 */
package de.tuberlin.ise.benchfoundry.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dave
 *
 *
 * adapted from: https://examples.javacodegeeks.com/core-java/util/concurrent/threadfactory/java-util-concurrent-threadfactory-example/
 *
 */
public class CustomThreadFactoryBuilder {

	private String namePrefix = null;
	private boolean daemon = false;

	public CustomThreadFactoryBuilder setNamePrefix(String namePrefix) {
		if (namePrefix == null) {
			throw new NullPointerException();
		}
		this.namePrefix = namePrefix;
		return this;
	}

	public CustomThreadFactoryBuilder setDaemon(boolean daemon) {
		this.daemon = daemon;
		return this;
	}

	

	public ThreadFactory build() {
		return build(this);
	}

	private static ThreadFactory build(CustomThreadFactoryBuilder builder) {
		final String namePrefix = builder.namePrefix;
		final Boolean daemon = builder.daemon;

		final AtomicLong count = new AtomicLong(0);

		return new ThreadFactory() {
			@Override
			public Thread newThread(Runnable runnable) {
				Thread thread = new Thread(runnable);
				if (namePrefix != null) {
					thread.setName(namePrefix + "-" + count.getAndIncrement());
				}
				if (daemon != null) {
					thread.setDaemon(daemon);
				}
				return thread;
			}
		};
	}
	
}
