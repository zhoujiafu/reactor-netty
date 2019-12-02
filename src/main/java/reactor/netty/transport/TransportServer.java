package reactor.netty.transport;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.util.AttributeKey;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import static reactor.netty.ReactorNetty.format;

/**
 * A genric {@link Transport} that will {@link #bind()} to a local address and provide a {@link DisposableServer}
 *
 * @param <T> TransportServer implementation
 * @param <SERV> DisposableServer implementation
 * @param <CONF> Client Configuration implementation
 *
 * @author Stephane Maldini
 */
public abstract class TransportServer<T extends TransportServer<T, SERV, CONF>,
		SERV extends DisposableServer,
		CONF extends TransportServerConfig<CONF, SERV>>
		extends Transport<T, CONF> {

	/**
	 * Binds the {@link TransportServer} and returns a {@link Mono} of {@link DisposableServer}. If
	 * {@link Mono} is cancelled, the underlying binding will be aborted. Once the {@link
	 * DisposableServer} has been emitted and is not necessary anymore, disposing the main server
	 * loop must be done by the user via {@link DisposableServer#dispose()}.
	 *
	 * @return a {@link Mono} of {@link DisposableServer}
	 */
	public abstract Mono<SERV> bind();

	/**
	 * Starts the server in a blocking fashion, and waits for it to finish initializing
	 * or the startup timeout expires (the startup timeout is {@code 45} seconds). The
	 * returned {@link DisposableServer} offers simple server API, including to {@link
	 * DisposableServer#disposeNow()} shut it down in a blocking fashion.
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public final SERV bindNow() {
		return bindNow(Duration.ofSeconds(45));
	}

	/**
	 * Start the server in a blocking fashion, and wait for it to finish initializing
	 * or the provided startup timeout expires. The returned {@link DisposableServer}
	 * offers simple server API, including to {@link DisposableServer#disposeNow()}
	 * shut it down in a blocking fashion.
	 *
	 * @param timeout max startup timeout
	 *
	 * @return a {@link DisposableServer}
	 *
	 * @return a {@link Connection}
	 */
	public final SERV bindNow(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		try {
			return Objects.requireNonNull(bind().block(timeout), "aborted");
		}
		catch (IllegalStateException e) {
			if (e.getMessage()
			     .contains("blocking read")) {
				throw new IllegalStateException(getClass().getSimpleName() + " couldn't be started within " + timeout.toMillis() + "ms");
			}
			throw e;
		}
	}
	
	/**
	 * Start the server in a fully blocking fashion, not only waiting for it to initialize
	 * but also blocking during the full lifecycle of the server. Since most
	 * servers will be long-lived, this is more adapted to running a server out of a main
	 * method, only allowing shutdown of the servers through {@code sigkill}.
	 * <p>
	 * Note: {@link Runtime#addShutdownHook(Thread) JVM shutdown hook} is added by
	 * this method in order to properly disconnect the server upon receiving a
	 * {@code sigkill} signal.</p>
	 *
	 * @param timeout a timeout for server shutdown
	 * @param onStart an optional callback on server start
	 */
	public final void bindUntilJavaShutdown(Duration timeout, @Nullable Consumer<DisposableServer> onStart) {
		Objects.requireNonNull(timeout, "timeout");
		DisposableServer facade = bindNow();

		Objects.requireNonNull(facade, "facade");

		if (onStart != null) {
			onStart.accept(facade);
		}

		Runtime.getRuntime()
		       .addShutdownHook(new Thread(() -> facade.disposeNow(timeout)));

		facade.onDispose()
		      .block();
	}

	/**
	 * Injects default attribute to the future child {@link Channel} connections. It
	 * will be available via {@link Channel#attr(AttributeKey)}.
	 * If the {@code value} is {@code null}, the attribute of the specified {@code key}
	 * is removed.
	 *
	 * @param key the attribute key
	 * @param value the attribute value - null to remove a key
	 * @param <A> the attribute type
	 *
	 * @return a new {@link TransportServer} reference
	 *
	 * @see ServerBootstrap#childAttr(AttributeKey, Object)
	 */
	public final <A> T childAttr(AttributeKey<A> key, @Nullable A value) {
		T dup = duplicate();
		dup.configuration().childAttrs = TransportConfig.updateMap(configuration().childAttrs, key, value);
		return dup;
	}

	/**
	 * Injects default options to the future child {@link Channel} connections. It
	 * will be available via {@link Channel#config()}.
	 * If the {@code value} is {@code null}, the attribute of the specified {@code key}
	 * is removed.
	 *
	 * @param key the option key
	 * @param value the option value - null to remove a key
	 * @param <A> the option type
	 *
	 * @return a new {@link TransportServer} reference
	 *
	 * @see ServerBootstrap#childOption(ChannelOption, Object)
	 */
	public final <A> T childOption(ChannelOption<A> key, @Nullable A value) {
		T dup = duplicate();
		dup.configuration().childOptions = TransportConfig.updateMap(configuration().childOptions, key, value);
		return dup;
	}

	/**
	 * Set or add the given {@link ConnectionObserver} for each remote connection
	 *
	 * @param observer the {@link ConnectionObserver} addition
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T childObserve(ConnectionObserver observer) {
		T dup = duplicate();
		dup.configuration().childObserver = observer.then(observer);
		return dup;
	}


	/**
	 * Setup a callback called when {@link TransportServer} is about to start listening for incoming traffic.
	 *
	 * @param doOnBind a consumer observing connected events
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T doOnBind(Consumer<? super CONF> doOnBind) {
		T dup = duplicate();
		Objects.requireNonNull(doOnBind, "doOnBind");
		@SuppressWarnings("unchecked")
		Consumer<CONF> current = (Consumer<CONF>)dup.configuration().doOnBind;
		dup.configuration().doOnBind = current == null ? doOnBind :current.andThen(doOnBind);
		return dup;
	}

	/**
	 * Setup a callback called after {@link DisposableServer} has been started.
	 *
	 * @param doOnBound a consumer observing connected events
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T doOnBound(Consumer<? super SERV> doOnBound) {
		T dup = duplicate();
		Objects.requireNonNull(doOnBound, "doOnBound");
		@SuppressWarnings("unchecked")
		Consumer<SERV> current = (Consumer<SERV>)dup.configuration().doOnBound;
		dup.configuration().doOnBound = current == null ? doOnBound : current.andThen(doOnBound);
		return dup;
	}

	/**
	 * Setup a callback called on new remote {@link Connection}.
	 *
	 * @param doOnConnection a consumer observing remote connections
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T doOnConnection(Consumer<? super Connection> doOnConnection) {
		T dup = duplicate();
		Objects.requireNonNull(doOnConnection, "doOnConnected");
		@SuppressWarnings("unchecked")
		Consumer<Connection> current = (Consumer<Connection>)dup.configuration().doOnConnection;
		dup.configuration().doOnConnection = current == null ? doOnConnection : current.andThen(doOnConnection);
		return dup;
	}

	/**
	 * Setup a callback called after {@link DisposableServer} has been shutdown.
	 *
	 * @param doOnUnbound a consumer observing unbound events
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T doOnUnbound(Consumer<? super SERV> doOnUnbound) {
		T dup = duplicate();
		Objects.requireNonNull(doOnUnbound, "doOnDisconnected");
		@SuppressWarnings("unchecked")
		Consumer<SERV> current = (Consumer<SERV>)dup.configuration().doOnUnbound;
		dup.configuration().doOnUnbound = current == null ? doOnUnbound :current.andThen(doOnUnbound);
		return dup;
	}

	/**
	 * Attach an IO handler to react on connected client
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link Publisher} terminates.
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T handle(BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler) {
		return doOnConnection(new OnConnectionHandle<>(handler));
	}

	/**
	 * The host to which this client should connect.
	 *
	 * @param host The host to connect to.
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T host(String host) {
		return localAddress(AddressUtils.updateHost(configuration().localAddress(), host));
	}

	/**
	 * The port to which this client should connect.
	 *
	 * @param port The port to connect to.
	 *
	 * @return a new {@link TransportServer} reference
	 */
	public final T port(int port) {
		return localAddress(AddressUtils.updatePort(configuration().localAddress(), port));
	}

	static final class OnConnectionHandle<CONN extends Connection> implements Consumer<CONN> {

		final BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler;

		public OnConnectionHandle(BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler) {
			this.handler = Objects.requireNonNull(handler, "handler");
		}

		@Override
		public void accept(CONN c) {
			if (log.isDebugEnabled()) {
				log.debug(format(c.channel(), "Handler is being applied: {}"), handler);
			}

			Mono.fromDirect(handler.apply(c.inbound(), c.outbound()))
			    .subscribe(c.disposeSubscriber());
		}
	}

	static final Logger log = Loggers.getLogger(TransportServer.class);
}
