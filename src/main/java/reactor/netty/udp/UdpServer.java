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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.InternetProtocolFamily;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportServer;

/**
 * A UdpServer allows to build in a safe immutable way a UDP server that is materialized
 * and connecting when {@link #bind(Bootstrap)} is ultimately called.
 * <p>
 * <p> Internally, materialization happens in two phases, first {@link #configure()} is
 * called to retrieve a ready to use {@link Bootstrap} then {@link #bind(Bootstrap)}
 * is called.
 * <p>
 * <p> Example:
 * <pre>
 * {@code
 * UdpServer.create()
 *          .doOnBind(startMetrics)
 *          .doOnBound(startedMetrics)
 *          .doOnUnbind(stopMetrics)
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .bindNow()
 * }
 *
 * @author Stephane Maldini
 */
public abstract class UdpServer extends TransportServer<UdpServer, UdpServerConnection, UdpServerConfig> {

	/**
	 * Prepare a {@link UdpServer}
	 *
	 * @return a {@link UdpServer}
	 */
	public static UdpServer create() {
		return UdpServerBind.INSTANCE;
	}

	/**
	 * Prepare a {@link UdpServer} from {@link UdpServerConfig}
	 *
	 * @return a {@link UdpServer}
	 */
	public static UdpServer create(UdpServerConfig config) {
		return new UdpServerBind(config);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources}
	 * container.
	 *
	 * @param channelResources a {@link LoopResources} accepting native runtime
	 * expectation and returning an eventLoopGroup.
	 * @param family a specific {@link InternetProtocolFamily} to run with
	 *
	 * @return a new {@link UdpServer}
	 */
	public final UdpServer runOn(LoopResources channelResources, InternetProtocolFamily family) {
		UdpServer dup = runOn(channelResources, false);
		dup.configuration().family = Objects.requireNonNull(family, "family");
		return dup;
	}

	@Override
	protected UdpServer duplicate() {
		return new UdpServerBind(new UdpServerConfig(configuration()));
	}
}
