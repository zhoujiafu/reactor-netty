package reactor.netty.udp;

import java.util.Collections;
import java.util.Map;

import io.netty.channel.ChannelOption;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportServerConfig;

/**
 * Encapsulate all necessary configuration for client UDP transport. The public API is read-only.
 */
public final class UdpServerConfig extends TransportServerConfig<UdpServerConfig, UdpServerConnection> {

	/**
	 * Return the default {@link UdpServerConfig} configuration
	 *
	 * @return the default {@link UdpServerConfig} configuration
	 */
	public static UdpServerConfig defaultServer() {
		return DEFAULT;
	}

	/**
	 * Return the configured {@link ChannelOperations.OnSetup}
	 *
	 * @return the configured {@link ChannelOperations.OnSetup}
	 */
	@Override
	public final ChannelOperations.OnSetup channelOperationsProvider() {
		return UDP_OPS;
	}

	// Package private creators
	InternetProtocolFamily family;

	UdpServerConfig() {
		super(DEFAULT_CHANNEL_OPTIONS, Collections.emptyMap());
	}

	UdpServerConfig(UdpServerConfig parent) {
		super(parent);
		parent.family = family;
	}

	@Override
	protected String defaultMetricsCategory() {
		return SERVER_METRICS_NAME;
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return UdpResources.get();
	}

	static final String                    SERVER_METRICS_NAME = "reactor.netty.udp.server";
	static final LoggingHandler            LOGGING_HANDLER     = new LoggingHandler(UdpServer.class);
	static final UdpServerConfig           DEFAULT             = new UdpServerConfig();
	static final ChannelOperations.OnSetup UDP_OPS             = (ch, c, msg) -> new UdpServerOperations(ch, c);

	static final Map<ChannelOption<?>, Object> DEFAULT_CHANNEL_OPTIONS;

	static {
		DEFAULT_CHANNEL_OPTIONS = Collections.singletonMap(ChannelOption.SO_REUSEADDR, true);
	}
}
