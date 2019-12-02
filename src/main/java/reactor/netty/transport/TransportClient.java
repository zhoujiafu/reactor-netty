package reactor.netty.transport;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.netty.resolver.AddressResolverGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.netty.ReactorNetty.format;

/**
 * A genric {@link Transport} that will {@link #connect()} to a remote address and provide a {@link Connection}
 *
 * @param <T> TransportClient implementation
 * @param <CONN> Connection implementation
 * @param <CONF> Client Configuration implementation
 *
 * @author Stephane Maldini
 */
public abstract class TransportClient<T extends TransportClient<T, CONN, CONF>,
		CONN extends Connection,
		CONF extends TransportClientConfig<CONF, CONN>>
		extends Transport<T, CONF> {

	/**
	 * Connect the {@link TransportClient} and return a {@link Mono} of {@link Connection}. If
	 * {@link Mono} is cancelled, the underlying connection will be aborted. Once the
	 * {@link Connection} has been emitted and is not necessary anymore, disposing must be
	 * done by the user via {@link Connection#dispose()}.
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public final Mono<CONN> connect() {
		Mono<CONN> connection = doConnect();

	}

	/**
	 * Block the {@link TransportClient} and return a {@link Connection}. Disposing must be
	 * done by the user via {@link Connection#dispose()}. The max connection
	 * timeout is 45 seconds.
	 *
	 * @return a {@link Connection}
	 */
	public final CONN connectNow() {
		return connectNow(Duration.ofSeconds(45));
	}

	/**
	 * Block the {@link TransportClient} and return a {@link Connection}. Disposing must be
	 * done by the user via {@link Connection#dispose()}.
	 *
	 * @param timeout connect timeout
	 *
	 * @return a {@link Connection}
	 */
	public final CONN connectNow(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		try {
			return Objects.requireNonNull(connect().block(timeout), "aborted");
		}
		catch (IllegalStateException e) {
			if (e.getMessage().contains("blocking read")) {
				throw new IllegalStateException(getClass().getSimpleName()+" couldn't be started within " + timeout.toMillis() + "ms");
			}
			throw e;
		}
	}

	/**
	 * Setup a callback called when {@link TransportClient} is about to connect to the remote endpoint.
	 *
	 * @param doOnConnect a consumer observing connected events
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnConnect(Consumer<? super CONF> doOnConnect) {
		T dup = duplicate();
		Objects.requireNonNull(doOnConnect, "doOnConnect");
		@SuppressWarnings("unchecked")
		Consumer<CONF> current = (Consumer<CONF>)dup.configuration().doOnConnect;
		dup.configuration().doOnConnect = current == null ? doOnConnect :current.andThen(doOnConnect);
		return dup;
	}

	/**
	 * Setup a callback called after {@link Connection} has been connected.
	 *
	 * @param doOnConnected a consumer observing connected events
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnConnected(Consumer<? super CONN> doOnConnected) {
		T dup = duplicate();
		Objects.requireNonNull(doOnConnected, "doOnConnected");
		@SuppressWarnings("unchecked")
		Consumer<CONN> current = (Consumer<CONN>)dup.configuration().doOnConnected;
		dup.configuration().doOnConnected = current == null ? doOnConnected : current.andThen(doOnConnected);
		return dup;
	}

	/**
	 * Setup a callback called after {@link Connection} has been disconnected.
	 *
	 * @param doOnDisconnected a consumer observing disconnected events
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T doOnDisconnected(Consumer<? super CONN> doOnDisconnected) {
		T dup = duplicate();
		Objects.requireNonNull(doOnDisconnected, "doOnDisconnected");
		@SuppressWarnings("unchecked")
		Consumer<CONN> current = (Consumer<CONN>)dup.configuration().doOnDisconnected;
		dup.configuration().doOnDisconnected = current == null ? doOnDisconnected :current.andThen(doOnDisconnected);
		return dup;
	}

	/**
	 * Attach an IO handler to react on connected client
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link org.reactivestreams.Publisher} terminates.
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T handle(BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler) {
		return doOnConnected(new OnConnectedHandle<>(handler));
	}

	/**
	 * The host to which this client should connect.
	 *
	 * @param host The host to connect to.
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T host(String host) {
		return remoteAddress(AddressUtils.updateHost(configuration().remoteAddress(), host));
	}

	/**
	 * Remove any previously applied Proxy configuration customization
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T noProxy() {
		if (configuration().hasProxy()) {
			T dup = duplicate();
			dup.configuration().proxyProvider = null;
			return dup;
		}
		@SuppressWarnings("unchecked")
		T dup = (T) this;
		return dup;
	}

	/**
	 * The port to which this client should connect.
	 *
	 * @param port The port to connect to.
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T port(int port) {
		return remoteAddress(AddressUtils.updatePort(configuration().remoteAddress(), port));
	}

	/**
	 * Apply a proxy configuration
	 *
	 * @param proxyOptions the proxy configuration callback
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T proxy(Consumer<? super ProxyProvider.TypeSpec> proxyOptions) {
		T dup = duplicate();
		dup.configuration().proxyProvider = ProxyProvider.create(proxyOptions);
		return dup;
	}

	/**
	 * Assign an {@link AddressResolverGroup}.
	 *
	 * @param resolver the new {@link AddressResolverGroup}
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T resolver(AddressResolverGroup<?> resolver) {
		T dup = duplicate();
		configuration().resolver = Objects.requireNonNull(resolver, "resolver");
		return dup;
	}

	/**
	 * The address to which this client should connect for each subscribe.
	 *
	 * @param connectAddressSupplier A {@link Mono} of the address to connect to. Each connect will subscribe and read an address
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T remoteAddress(Mono<? extends SocketAddress> connectAddressSupplier) {
		return remoteAddress(AddressUtils.lazyAddress(connectAddressSupplier));
	}

	/**
	 * The address to which this client should connect for each subscribe.
	 *
	 * @param remoteAddress the address to connect to.
	 *
	 * @return a new {@link TransportClient} reference
	 */
	public final T remoteAddress(SocketAddress remoteAddress) {
		T dup = duplicate();
		duplicate().configuration().remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
		return dup;
	}

	static final class OnConnectedHandle<CONN extends Connection> implements Consumer<CONN> {

		final BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler;

		public OnConnectedHandle(BiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler) {
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

	static final Logger log = Loggers.getLogger(TransportClient.class);
}
