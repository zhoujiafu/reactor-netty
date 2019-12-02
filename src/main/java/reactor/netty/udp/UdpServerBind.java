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

import java.util.Objects;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

/**
 * @author Stephane Maldini
 */
final class UdpServerBind extends UdpServer {

	static final UdpServerBind INSTANCE = new UdpServerBind(UdpServerConfig.defaultServer());

	final UdpServerConfig config;

	UdpServerBind(UdpServerConfig config) {
		this.config = Objects.requireNonNull(config, "config");
	}

	@Override
	public Mono<UdpServerConnection> bind() {

		//Default group and channel
		if (b.config()
		     .group() == null) {

			UdpResources loopResources = UdpResources.get();
			EventLoopGroup elg = loopResources.onClient(LoopResources.DEFAULT_NATIVE);

			b.group(elg)
			 .channel(loopResources.onDatagramChannel(elg));
		}

		Bootstrap b = source.configure();

		boolean useNative = family == null && preferNative;

		EventLoopGroup elg = loopResources.onClient(useNative);

		if (useNative) {
			b.channel(loopResources.onDatagramChannel(elg));
		}
		else {
			b.channelFactory(() -> new NioDatagramChannel(family));
		}

		return ConnectionProvider.newConnection()
		                         .acquire(b);
	}
}
