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
import reactor.netty.Connection;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportClient;

/**
 * A UdpClient allows to build in a safe immutable way a UDP client that is materialized
 * and connecting when {@link #connect(Bootstrap)} is ultimately called.
 * <p>
 * <p> Internally, materialization happens in two phases, first {@link #configure()} is
 * called to retrieve a ready to use {@link Bootstrap} then {@link #connect(Bootstrap)}
 * is called.
 * <p>
 * <p> Example:
 * <pre>
 * {@code
 * UdpClient.create()
 *          .doOnConnect(startMetrics)
 *          .doOnConnected(startedMetrics)
 *          .doOnDisconnected(stopMetrics)
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .connectNow();
 * }
 *
 * @author Stephane Maldini
 */
public abstract class UdpClient extends TransportClient<UdpClient, Connection, UdpClientConfig> {

	/**
	 * Prepare a {@link UdpClient}
	 *
	 * @return a {@link UdpClient}
	 */
	public static UdpClient create() {
		return UdpClientConnect.INSTANCE;
	}

	/**
	 * Prepare a {@link UdpClient} from a given {@link UdpClientConfig}
	 *
	 * @return a {@link UdpClient}
	 */
	public static UdpClient create(UdpClientConfig config) {
		return new UdpClientConnect(config);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources}
	 * container.
	 *
	 * @param channelResources a {@link LoopResources} accepting native runtime
	 * expectation and returning an eventLoopGroup.
	 * @param family a specific {@link InternetProtocolFamily} to run with
	 *
	 * @return a new {@link UdpClient}
	 */
	public final UdpClient runOn(LoopResources channelResources, InternetProtocolFamily family) {
		UdpClient dup = runOn(channelResources, false);
		dup.configuration().family = Objects.requireNonNull(family, "family");
		return dup;
	}

	@Override
	protected UdpClient duplicate() {
		return new UdpClientConnect(new UdpClientConfig(configuration()));
	}
}
