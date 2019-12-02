/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.resources;

import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.ReactorNetty;

/**
 * An {@link EventLoopGroup} selector with associated
 * {@link io.netty.channel.Channel} factories.
 *
 * @author Stephane Maldini
 * @since 0.6
 */
@FunctionalInterface
public interface LoopResources extends Disposable {

	/**
	 * Default worker thread count, fallback to available processor
	 * (but with a minimum value of 4)
	 */
	int DEFAULT_IO_WORKER_COUNT = Integer.parseInt(System.getProperty(
			ReactorNetty.IO_WORKER_COUNT,
			"" + Math.max(Runtime.getRuntime()
			            .availableProcessors(), 4)));
	/**
	 * Default selector thread count, fallback to -1 (no selector thread)
	 */
	int DEFAULT_IO_SELECT_COUNT = Integer.parseInt(System.getProperty(
			ReactorNetty.IO_SELECT_COUNT,
			"" + -1));

	/**
	 * Default value whether the native transport (epoll, kqueue) will be preferred,
	 * fallback it will be preferred when available
	 */
	boolean DEFAULT_NATIVE = Boolean.parseBoolean(System.getProperty(
			ReactorNetty.NATIVE,
			"true"));

	/**
	 * Create a delegating {@link EventLoopGroup} which reuse local event loop if already
	 * working
	 * inside one.
	 *
	 * @param group the {@link EventLoopGroup} to decorate
	 *
	 * @return a decorated {@link EventLoopGroup} that will colocate executions on the
	 * same thread stack
	 */
	static EventLoopGroup colocate(EventLoopGroup group) {
		return new ColocatedEventLoopGroup(group);
	}

	/**
	 * Create a simple {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 *
	 * @param prefix the event loop thread name prefix
	 * @param workerCount number of worker threads
	 * @param daemon should the thread be released on jvm shutdown
	 *
	 * @return a new {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 */
	static LoopResources create(String prefix, int workerCount, boolean daemon) {
		if (workerCount < 1) {
			throw new IllegalArgumentException("Must provide a strictly positive " + "worker threads number, " + "was: " + workerCount);
		}
		return new DefaultLoopResources(prefix, workerCount, daemon);
	}

	/**
	 * Create a simple {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 *
	 * @param prefix the event loop thread name prefix
	 * @param selectCount number of selector threads
	 * @param workerCount number of worker threads
	 * @param daemon should the thread be released on jvm shutdown
	 *
	 * @return a new {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 */
	static LoopResources create(String prefix,
			int selectCount,
			int workerCount,
			boolean daemon) {
		if (Objects.requireNonNull(prefix, "prefix")
		           .isEmpty()) {
			throw new IllegalArgumentException("Cannot use empty prefix");
		}
		if (workerCount < 1) {
			throw new IllegalArgumentException("Must provide a strictly positive " + "worker threads number, " + "was: " + workerCount);
		}
		if (selectCount < 1) {
			throw new IllegalArgumentException("Must provide a strictly positive " + "selector threads number, " + "was: " + selectCount);
		}
		return new DefaultLoopResources(prefix, selectCount, workerCount, daemon);
	}

	/**
	 * Create a simple {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 *
	 * @param prefix the event loop thread name prefix
	 *
	 * @return a new {@link LoopResources} to provide automatically for {@link
	 * EventLoopGroup} and {@link Channel} factories
	 */
	static LoopResources create(String prefix) {
		return new DefaultLoopResources(prefix, DEFAULT_IO_SELECT_COUNT,
				DEFAULT_IO_WORKER_COUNT,
				true);
	}

	/**
	 * Callback for client {@link EventLoopGroup} creation.
	 *
	 * @param useNative should use native group if current environment supports it
	 *
	 * @return a new {@link EventLoopGroup}
	 */
	default EventLoopGroup onClient(boolean useNative) {
		return onServer(useNative);
	}

	/**
	 * Callback for server {@link EventLoopGroup} creation.
	 *
	 * @param useNative should use native group if current environment supports it
	 *
	 * @return a new {@link EventLoopGroup}
	 */
	EventLoopGroup onServer(boolean useNative);

	/**
	 * Callback for server channel factory selection.
	 *
	 * @param channelType the  {@link Channel} type to create
	 *
	 * @return a {@link Channel} target or throw an {@link IllegalArgumentException} if unsupported
	 */
	default <CHANNEL extends Channel> CHANNEL onChannel(Class<CHANNEL> channelType, EventLoopGroup group) {
		DefaultLoop channelFactory = DefaultLoopNativeDetector.INSTANCE.supportGroup(group) ?
				DefaultLoopNativeDetector.INSTANCE :
				DefaultLoopNativeDetector.NIO;

		return channelFactory.getChannel(channelType);
	}

	/**
	 * Create a server select {@link EventLoopGroup} for servers to be used
	 *
	 * @param useNative should use native group if current environment supports it
	 *
	 * @return a new {@link EventLoopGroup}
	 */
	default EventLoopGroup onServerSelect(boolean useNative) {
		return onServer(useNative);
	}

	/**
	 * return true if {@link EventLoopGroup} should not be shutdown
	 *
	 * @return true if {@link EventLoopGroup} should not be shutdown
	 */
	default boolean daemon() {
		return false;
	}

	/**
	 * Dispose the underlying LoopResources.
	 * This method is NOT blocking. It is implemented as fire-and-forget.
	 * Use {@link #disposeLater()} when you need to observe the final
	 * status of the operation, combined with {@link Mono#block()}
	 * if you need to synchronously wait for the underlying resources to be disposed.
	 */
	@Override
	default void dispose() {
		//noop default
		disposeLater().subscribe();
	}

	/**
	 * Returns a Mono that triggers the disposal of the underlying LoopResources when subscribed to.
	 *
	 * @return a Mono representing the completion of the LoopResources disposal.
	 **/
	default Mono<Void> disposeLater() {
		//noop default
		return Mono.empty();
	}

	/**
	 * Return true if environment supports native connections
	 *
	 * @return true if environment supports native connections
	 */
	static boolean hasNativeSupport() {
		return DefaultLoopNativeDetector.INSTANCE != DefaultLoopNativeDetector.NIO;
	}
}
