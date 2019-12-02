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

import java.util.Objects;
import java.util.function.Consumer;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.transport.TransportServer;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A TcpServer allows to build in a safe immutable way a TCP server that is materialized
 * and connecting when {@link #bind(ServerBootstrap)} is ultimately called.
 * <p> Internally, materialization happens in two phases:</p>
 * <ul>
 * <li>first {@link #configure()} is called to retrieve a ready to use {@link ServerBootstrap}</li>
 * <li>then {@link #bind(ServerBootstrap)} is called.</li>
 * </ul>
 * <p> Example:</p>
 * <pre>
 * {@code
 * TcpServer.create()
 *          .doOnBind(startMetrics)
 *          .doOnBound(startedMetrics)
 *          .doOnUnbound(stopMetrics)
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .bindNow()
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public abstract class TcpServer extends TransportServer<TcpServer, DisposableServer, TcpServerConfig> {

	/**
	 * Prepare a {@link TcpServer}
	 *
	 * @return a new {@link TcpServer}
	 */
	public static TcpServer create() {
		return TcpServerBind.INSTANCE;
	}

	/**
	 * Prepare a {@link TcpServer}
	 *
	 * @param config a {@link TcpServerConfig} to configure the underlying server
	 *
	 * @return a {@link TcpServer}
	 */
	public static TcpServer create(TcpServerConfig config) {
		return new TcpServerBind(config);
	}

	/**
	 * Removes any previously applied SSL configuration customization
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer noSSL() {
		if (configuration().sslProvider() == null) {
			return this;
		}
		TcpServer dup = duplicate();
		dup.configuration().sslProvider = null;
		return dup;
	}

	/**
	 * Apply an SSL configuration customization via the passed builder. The builder
	 * will produce the {@link SslContext} to be passed to with a default value of
	 * {@code 10} seconds handshake timeout unless the environment property {@code
	 * reactor.netty.tcp.sslHandshakeTimeout} is set.
	 *
	 * If {@link SelfSignedCertificate} needs to be used, the sample below can be
	 * used. Note that {@link SelfSignedCertificate} should not be used in production.
	 * <pre>
	 * {@code
	 *     SelfSignedCertificate cert = new SelfSignedCertificate();
	 *     SslContextBuilder sslContextBuilder =
	 *             SslContextBuilder.forServer(cert.certificate(), cert.privateKey());
	 *     secure(sslContextSpec -> sslContextSpec.sslContext(sslContextBuilder));
	 * }
	 * </pre>
	 *
	 * @param sslProviderBuilder builder callback for further customization of SslContext.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure(Consumer<? super SslProvider.SslContextSpec> sslProviderBuilder) {
		return TcpServerSecure.secure(this, sslProviderBuilder);
	}

	/**
	 * Applies an SSL configuration via the passed {@link SslProvider}.
	 *
	 * If {@link SelfSignedCertificate} needs to be used, the sample below can be
	 * used. Note that {@link SelfSignedCertificate} should not be used in production.
	 * <pre>
	 * {@code
	 *     SelfSignedCertificate cert = new SelfSignedCertificate();
	 *     SslContextBuilder sslContextBuilder =
	 *             SslContextBuilder.forServer(cert.certificate(), cert.privateKey());
	 *     secure(sslContextSpec -> sslContextSpec.sslContext(sslContextBuilder));
	 * }
	 * </pre>
	 *
	 * @param sslProvider The provider to set when configuring SSL
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure(SslProvider sslProvider) {
		Objects.requireNonNull(sslProvider, "sslProvider");
		TcpServer dup = duplicate();
		dup.configuration().sslProvider = sslProvider;
		return dup;
	}

	@Override
	protected TcpServer duplicate() {
		return new TcpServerBind(new TcpServerConfig(configuration()));
	}


	static final int                   DEFAULT_PORT      = 0;
	static final LoggingHandler        LOGGING_HANDLER   = new LoggingHandler(TcpServer.class);
	static final Logger                log               = Loggers.getLogger(TcpServer.class);
}
