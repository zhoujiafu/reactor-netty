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

package reactor.netty.tcp;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import io.netty.handler.ssl.SslContext;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.TransportClient;

/**
 * A TcpClient allows to build in a safe immutable way a TCP client that
 * is materialized and connecting when {@link #connect(Bootstrap)} is ultimately called.
 *
 * <p> Internally, materialization happens in two phases, first {@link #configure()} is
 * called to retrieve a ready to use {@link Bootstrap} then {@link #connect(Bootstrap)}
 * is called.
 *
 * <p> Example:
 * <pre>
 * {@code
 * TcpClient.create()
 *          .doOnConnect(connectMetrics)
 *          .doOnConnected(connectedMetrics)
 *          .doOnDisconnected(disconnectedMetrics)
 *          .secure()
 *          .noSsl()
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .connectNow()
 * }
 *
 * @author Stephane Maldini
 */
public abstract class TcpClient extends TransportClient<TcpClient, Connection, TcpClientConfig> {

	/**
	 * Prepare a pooled {@link TcpClient}
	 *
	 * @return a {@link TcpClient}
	 */
	public static TcpClient create() {
		return create(TcpResources.get());
	}

	/**
	 * Prepare a {@link TcpClient}
	 *
	 * @param provider a {@link ConnectionProvider} to acquire connections
	 *
	 * @return a {@link TcpClient}
	 */
	public static TcpClient create(ConnectionProvider provider) {
		return new TcpClientConnect(provider);
	}

	/**
	 * Prepare a non pooled {@link TcpClient}
	 *
	 * @return a {@link TcpClient}
	 */
	public static TcpClient newConnection() {
		return TcpClientConnect.INSTANCE;
	}

	/**
	 * The address to which this client should connect for each subscribe.
	 *
	 * @param connectAddressSupplier A supplier of the address to connect to.
	 *
	 * @return a new {@link TcpClient}
	 * @deprecated use {@link #remoteAddress(Mono)}
	 */
	@Deprecated
	public final TcpClient addressSupplier(Supplier<? extends SocketAddress> connectAddressSupplier) {
		return remoteAddress(Mono.fromSupplier(connectAddressSupplier));
	}

	/**
	 * Remove any previously applied SSL configuration customization
	 *
	 * @return a new {@link TcpClient}
	 */
	public final TcpClient noSSL() {
		if (configuration().sslProvider() == null) {
			return this;
		}
		TcpClient dup = duplicate();
		dup.configuration().sslProvider = null;
		return dup;
	}

	/**
	 * Enable default sslContext support. The default {@link SslContext} will be
	 * assigned to
	 * with a default value of {@code 10} seconds handshake timeout unless
	 * the environment property {@code reactor.netty.tcp.sslHandshakeTimeout} is set.
	 *
	 * @return a new {@link TcpClient}
	 */
	public final TcpClient secure() {
		return secure(TcpClientConfig.defaultSslProvider());
	}

	/**
	 * Apply an SSL configuration customization via the passed builder. The builder
	 * will produce the {@link SslContext} to be passed to with a default value of
	 * {@code 10} seconds handshake timeout unless the environment property {@code
	 * reactor.netty.tcp.sslHandshakeTimeout} is set.
	 *
	 * @param sslProviderBuilder builder callback for further customization of SslContext.
	 *
	 * @return a new {@link TcpClient}
	 */
	public final TcpClient secure(Consumer<? super SslProvider.SslContextSpec> sslProviderBuilder) {
		return TcpClientSecure.secure(this, sslProviderBuilder);
	}

	/**
	 * Apply an SSL configuration via the passed {@link SslProvider}.
	 *
	 * @param sslProvider The provider to set when configuring SSL
	 *
	 * @return a new {@link TcpClient}
	 */
	public final TcpClient secure(SslProvider sslProvider) {
		Objects.requireNonNull(sslProvider, "sslProvider");
		TcpClient dup = duplicate();
		dup.configuration().sslProvider = sslProvider;
		return dup;
	}

	@Override
	protected final TcpClient duplicate() {
		return new TcpClientConnect(new TcpClientConfig(configuration()));
	}
}
