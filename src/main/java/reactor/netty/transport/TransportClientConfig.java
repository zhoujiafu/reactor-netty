package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Consumer;

import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.annotation.Nullable;

/**
 * Encapsulate all necessary configuration for client TCP transport. The public API is read-only.
 *
 * @param <CONF> Configuration implementation
 * @param <CONN> Connection implementation
 *
 */
public abstract class TransportClientConfig<CONF extends TransportConfig, CONN extends Connection> extends TransportConfig {

	/**
	 * Return the {@link ConnectionProvider}
	 *
	 * @return the {@link ConnectionProvider}
	 */
	public final ConnectionProvider connectionProvider() {
		return connectionProvider;
	}

	/**
	 * Return true if that {@link TransportClientConfig} is configured with a proxy
	 *
	 * @return true if that {@link TransportClientConfig} is configured with a proxy
	 */
	public final boolean hasProxy() {
		return proxyProvider != null;
	}

	/**
	 * Return the {@link ProxyProvider} if any
	 *
	 * @return the {@link ProxyProvider} if any
	 */
	@Nullable
	public final ProxyProvider proxyProvider() {
		return proxyProvider;
	}

	/**
	 * Return the {@link AddressResolverGroup} if any
	 *
	 * @return the {@link AddressResolverGroup} if any
	 */
	@Nullable
	public final AddressResolverGroup<?> resolver() {
		return resolver;
	}

	/**
	 * Return the remote configured {@link SocketAddress} or empty
	 *
	 * @return the remote configured {@link SocketAddress} or empty
	 */
	@Nullable
	public final SocketAddress remoteAddress() {
		return remoteAddress;
	}

	// Package private creators

	final ConnectionProvider     connectionProvider;

	ProxyProvider           proxyProvider;
	Consumer<? super CONF>  doOnConnect;
	Consumer<? super CONN>  doOnConnected;
	Consumer<? super CONN>  doOnDisconnected;
	SocketAddress           remoteAddress;
	AddressResolverGroup<?> resolver;

	protected TransportClientConfig(ConnectionProvider connectionProvider) {
		this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
		this.resolver = DefaultAddressResolverGroup.INSTANCE;
	}

	protected TransportClientConfig(TransportClientConfig<CONF, CONN> parent) {
		super(parent);
		this.connectionProvider = parent.connectionProvider;
		this.proxyProvider = parent.proxyProvider;
		this.doOnConnect = parent.doOnConnect;
		this.doOnConnected = parent.doOnConnected;
		this.doOnDisconnected = parent.doOnDisconnected;
		this.remoteAddress = parent.remoteAddress;
		this.resolver = parent.resolver;
	}
}
