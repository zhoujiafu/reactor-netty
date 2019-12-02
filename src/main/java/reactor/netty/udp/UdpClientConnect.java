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
package reactor.netty.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

/**
 * @author Stephane Maldini
 */
final class UdpClientConnect extends UdpClient {

	static final UdpClientConnect INSTANCE = new UdpClientConnect();

	@Override
	public UdpClientConfig configuration() {
		return config;
	}

	@Override
	public Mono<Connection> connect() {
		Bootstrap b = new Bootstrap();

		//TODO configure the operation provider - should remove
		BootstrapHandlers.channelOperationFactory(b, config.channelOperationsProvider());

			UdpResources loopResources = UdpResources.get();
			EventLoopGroup elg = loopResources.onClient(LoopResources.DEFAULT_NATIVE);

		// Remote address
		b.remoteAddress(config.remoteAddress());

		// Resolver
		b.resolver(config.resolver());

		//configure event loops and channel factory
		LoopResources r = config.loopResources();
		boolean preferNative;

		// No resources use the shared static TcpResources
		if (r == null) {
			r = UdpResources.get();
			preferNative = LoopResources.DEFAULT_NATIVE;
		}
		else{
			preferNative = config.isPreferNative();
		}

		EventLoopGroup elg = r.onClient(preferNative);
		b.group(elg)
		 .channel(r.onDatagramChannel(elg));

		Bootstrap b = source.configure();

		boolean useNative = family == null && preferNative;

		EventLoopGroup elg = loopResources.onClient(useNative);

		if (useNative) {
			b.channel(loopResources.onDatagramChannel(elg));
		}
		else {
			b.channelFactory(() -> new NioDatagramChannel(family));
		}

		return b.group(elg);

		//transfer options if any
		Map<ChannelOption<Object>, Object> options = config.options();
		if (!options.isEmpty()) {
			for(Map.Entry<ChannelOption<Object>, Object> option : options.entrySet()) {
				b.option(option.getKey(), option.getValue());
			}
		}

		return ConnectionProvider.newConnection()
		                         .acquire(b);
	}
}
