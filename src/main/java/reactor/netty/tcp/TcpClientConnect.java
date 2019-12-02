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
import java.util.Map;
import java.util.Objects;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.AttributeKey;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.AddressResolverGroupMetrics;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.ProxyProvider;

/**
 * @author Stephane Maldini
 */
final class TcpClientConnect extends TcpClient {

	static final TcpClientConnect INSTANCE = new TcpClientConnect(ConnectionProvider.newConnection());

	final ConnectionProvider provider;

	TcpClientConnect(ConnectionProvider provider) {
		this.provider = Objects.requireNonNull(provider, "connectionProvider");
	}

	@Override
	public TcpClientConfig configuration() {
		return config;
	}

	@Override
	public Mono<Connection> connect() {
		Bootstrap b = new Bootstrap();

		//TODO configure the operation provider - should remove
		BootstrapHandlers.channelOperationFactory(b, config.channelOperationsProvider());

		// Local address
		b.localAddress(config.localAddress());

		// Remote address
		b.remoteAddress(config.remoteAddress());

		// Resolver
		b.resolver(config.resolver());

		//configure event loops and channel factory
		LoopResources r = config.loopResources();
		boolean preferNative;

		// No resources use the shared static TcpResources
		if (r == null) {
			r = TcpResources.get();
			preferNative = LoopResources.DEFAULT_NATIVE;
		}
		else{
			preferNative = config.isPreferNative();
		}

		EventLoopGroup elg = r.onClient(preferNative);
		b.group(elg)
		 .channel(r.onChannel(elg));

		//transfer options if any
		Map<ChannelOption<Object>, Object> options = config.options();
		if (!options.isEmpty()) {
			for(Map.Entry<ChannelOption<Object>, Object> option : options.entrySet()) {
				b.option(option.getKey(), option.getValue());
			}
		}

		//transfer attributes if any
		Map<AttributeKey<Object>, Object> attributes = config.attributes();
		if (!attributes.isEmpty()) {
			for(Map.Entry<AttributeKey<Object>, Object> attribute : attributes.entrySet()) {
				b.attr(attribute.getKey(), attribute.getValue());
			}
		}

		// Configure proxy if any
		if (config.proxyProvider() != null) {
			BootstrapHandlers.updateConfiguration(b, NettyPipeline.ProxyHandler, new ProxyProvider.DeferredProxySupport(config.proxyProvider()));

		if (b.config()
		     .group() == null) {

			TcpClientRunOn.configure(b,
					LoopResources.DEFAULT_NATIVE,
					TcpResources.get());
		}

		return provider.acquire(b);

		// Configure metrics if any
		if (config.metricsCategory() != null) {
			BootstrapHandlers.updateMetricsSupport(b, config.metricsCategory(), "tcp");

			b.resolver(new AddressResolverGroupMetrics((AddressResolverGroup<SocketAddress>) b.config().resolver()));
		}

		// Configure tcp event observer if any
		ConnectionObserver observer = config.connectionObserver();

		if (config.doOnConnected != null || config.doOnDisconnected != null) {
			observer = observer.then((connection, newState) -> {
				if (config.doOnConnected != null && newState == ConnectionObserver.State.CONFIGURED) {
					config.doOnConnected.accept(connection);
					return;
				}
				if (config.doOnDisconnected != null) {
					if (newState == ConnectionObserver.State.DISCONNECTING) {
						connection.onDispose(() -> config.doOnDisconnected.accept(connection));
					}
					else if (newState == ConnectionObserver.State.RELEASED) {
						config.doOnDisconnected.accept(connection);
					}
				}
			});
		}

		if (observer != ConnectionObserver.emptyListener()) {
			BootstrapHandlers.connectionObserver(b, observer);
		}

		// Prepare connection acquisition from the provider
		Mono<? extends Connection> connection = config.connectionProvider()
		                                              .acquire(b);

		// If doOnConnect is configured
		if (config.doOnConnect != null) {
			return connection.doOnSubscribe(s -> config.doOnConnect.accept(config));
		}

		return connection;
	}
}
