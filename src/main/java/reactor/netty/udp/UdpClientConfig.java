package reactor.netty.udp;

import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.Connection;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportClientConfig;

/**
 * Encapsulate all necessary configuration for client UDP transport. The public API is read-only.
 */
public final class UdpClientConfig extends TransportClientConfig<UdpClientConfig, Connection> {

	/**
	 * Return the default {@link UdpClientConfig} configuration
	 *
	 * @return the default {@link UdpClientConfig} configuration
	 */
	public static UdpClientConfig defaultClient() {
		return DEFAULT;
	}

	@Override
	protected String defaultMetricsCategory() {
		return CLIENT_METRICS_NAME;
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return UdpResources.get();
	}

	// Package private creators

	InternetProtocolFamily family;

	UdpClientConfig(ConnectionProvider connectionProvider){
		super(connectionProvider);
	}

	UdpClientConfig(UdpClientConfig parent) {
		super(parent);
		this.family = parent.family;
	}

	static final String          CLIENT_METRICS_NAME = "reactor.netty.udp.client";
	static final LoggingHandler  LOGGING_HANDLER     = new LoggingHandler(UdpClient.class);
	static final UdpClientConfig DEFAULT             = new UdpClientConfig(ConnectionProvider.newConnection());
}
