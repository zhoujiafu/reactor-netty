package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Map;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.FailedChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLogger;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;

/**
 * @author Stephane Maldini
 */
public class TransportConnector {

	static <C extends Connection> Mono<C> connect(TransportClientConfig<?, C> clientConfig) {

		final SocketAddress remoteAddress = clientConfig.remoteAddress;
		final SocketAddress localAddress = clientConfig.localAddress;

		final ChannelFuture regFuture = initAndRegister(clientConfig.loopResources);
		final Channel channel = regFuture.channel();

		if (regFuture.isDone()) {
			if (!regFuture.isSuccess()) {
				return regFuture;
			}
			return doResolveAndConnect0(channel, remoteAddress, localAddress, channel.newPromise());
		}
		else {
			// Registration future is almost always fulfilled already, but just in case it's not.
			final AbstractBootstrap.PendingRegistrationPromise promise =
					new AbstractBootstrap.PendingRegistrationPromise(channel);
			regFuture.addListener(future -> {
				// Directly obtain the cause and do a null check so we only need one volatile read in case of a
				// failure.
				Throwable cause = future.cause();
				if (cause != null) {
					// Registration on the EventLoop failed so fail the ChannelPromise directly to not cause an
					// IllegalStateException once we try to access the EventLoop of the Channel.
					promise.setFailure(cause);
				}
				else {
					// Registration was successful, so set the correct executor to use.
					// See https://github.com/netty/netty/issues/2586
					promise.registered();
					doResolveAndConnect0(channel, remoteAddress, localAddress, promise);
				}
			});
			return promise;
		}
	}

	static ChannelFuture doResolveAndConnect0(final Channel channel,
			SocketAddress remoteAddress,
			final SocketAddress localAddress,
			final ChannelPromise promise) {
		try {
			final EventLoop eventLoop = channel.eventLoop();
			final AddressResolver<SocketAddress> resolver = this.resolver.getResolver(eventLoop);

			if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
				// Resolver has no idea about what to do with the specified remote address or it's resolved already.
				doConnect(remoteAddress, localAddress, promise);
				return promise;
			}

			final Future<SocketAddress> resolveFuture = resolver.resolve(remoteAddress);

			if (resolveFuture.isDone()) {
				final Throwable resolveFailureCause = resolveFuture.cause();

				if (resolveFailureCause != null) {
					// Failed to resolve immediately
					channel.close();
					promise.setFailure(resolveFailureCause);
				}
				else {
					// Succeeded to resolve immediately; cached? (or did a blocking lookup)
					doConnect(resolveFuture.getNow(), localAddress, promise);
				}
				return promise;
			}

			// Wait until the name resolution is finished.
			resolveFuture.addListener((FutureListener<SocketAddress>) future -> {
				if (future.cause() != null) {
					channel.close();
					promise.setFailure(future.cause());
				}
				else {
					doConnect(future.getNow(), localAddress, promise);
				}
			});
		}
		catch (Throwable cause) {
			promise.tryFailure(cause);
		}
		return promise;
	}

	static ChannelFuture initAndRegister(TransportClientConfig<?, ?> config) {
		LoopResources r = config.loopResources == null ? config.defaultLoopResources() : config.loopResources;
		boolean preferNative = config.preferNative;
		EventLoopGroup elg = r.onClient(preferNative);
		elg.next()
		Channel channel = null;
		try {
			channel = r.onChannel(config.channelType, elg);
			init(channel);
		}
		catch (Throwable t) {
			if (channel != null) {
				// channel can be null if newChannel crashed (eg SocketException("too many open files"))
				channel.unsafe()
				       .closeForcibly();
				// as the Channel is not registered yet we need to force the usage of the GlobalEventExecutor
				return new DefaultChannelPromise(channel, GlobalEventExecutor.INSTANCE).setFailure(t);
			}
			// as the Channel is not registered yet we need to force the usage of the GlobalEventExecutor
			return new DefaultChannelPromise(new FailedChannel(), GlobalEventExecutor.INSTANCE).setFailure(t);
		}

		ChannelFuture regFuture = elg.register(channel);

		if (regFuture.cause() != null) {
			if (channel.isRegistered()) {
				channel.close();
			}
			else {
				channel.unsafe()
				       .closeForcibly();
			}
		}

		// If we are here and the promise is not failed, it's one of the following cases:
		// 1) If we attempted registration from the event loop, the registration has been completed at this point.
		//    i.e. It's safe to attempt bind() or connect() now because the channel has been registered.
		// 2) If we attempted registration from the other thread, the registration request has been successfully
		//    added to the event loop's task queue for later execution.
		//    i.e. It's safe to attempt bind() or connect() now:
		//         because bind() or connect() will be executed *after* the scheduled registration task is executed
		//         because register(), bind(), and connect() are all bound to the same thread.

		return regFuture;
	}

	static void doConnect(final SocketAddress remoteAddress,
			final SocketAddress localAddress,
			final ChannelPromise connectPromise) {

		// This method is invoked before channelRegistered() is triggered.  Give user handlers a chance to set up
		// the pipeline in its channelRegistered() implementation.
		final Channel channel = connectPromise.channel();
		channel.eventLoop()
		       .execute(() -> {
			       if (localAddress == null) {
				       channel.connect(remoteAddress, connectPromise);
			       }
			       else {
				       channel.connect(remoteAddress, localAddress, connectPromise);
			       }
			       connectPromise.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
		       });
	}

	static void init(Channel channel) throws Exception {
		ChannelPipeline p = channel.pipeline();
		p.addLast(config.handler());

		final Map<ChannelOption<?>, Object> options = options0();
		synchronized (options) {
			setChannelOptions(channel, options, logger);
		}

		final Map<AttributeKey<?>, Object> attrs = attrs0();
		synchronized (attrs) {
			for (Map.Entry<AttributeKey<?>, Object> e : attrs.entrySet()) {
				channel.attr((AttributeKey<Object>) e.getKey())
				       .set(e.getValue());
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static void setChannelOption(
			Channel channel, ChannelOption<?> option, Object value, InternalLogger logger) {
		try {
			if (!channel.config().setOption((ChannelOption<Object>) option, value)) {
				logger.warn("Unknown channel option '{}' for channel '{}'", option, channel);
			}
		} catch (Throwable t) {
			logger.warn(
					"Failed to set channel option '{}' with value '{}' for channel '{}'", option, value, channel, t);
		}
	}

}
