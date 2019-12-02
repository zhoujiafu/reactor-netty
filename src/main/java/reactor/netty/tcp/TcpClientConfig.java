package reactor.netty.tcp;

import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.netty.Connection;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportClientConfig;
import reactor.util.annotation.Nullable;

/**
 * Encapsulate all necessary configuration for client TCP transport. The public API is read-only.
 */
public final class TcpClientConfig extends TransportClientConfig<TcpClientConfig, Connection> {

	/**
	 * Return the default {@link TcpClientConfig} configuration
	 *
	 * @return the default {@link TcpClientConfig} configuration
	 */
	public static TcpClientConfig defaultClient() {
		return DEFAULT;
	}

	/**
	 * Return true if that {@link TcpClientConfig} is set to be secured via SSL transport
	 *
	 * @return true if that {@link TcpClientConfig} is set to be secured via SSL transport
	 */
	public final boolean isSecure() {
		return sslProvider != null;
	}

	/**
	 * Return the configured {@link SslProvider} or null if no SSL configuration is provided
	 *
	 * @return the configured {@link SslProvider} or null if no SSL configuration is provided
	 */
	@Nullable
	public final SslProvider sslProvider() {
		return sslProvider;
	}

	// Package private creators

	SslProvider sslProvider;

	TcpClientConfig(ConnectionProvider connectionProvider) {
		super(connectionProvider);
		this.sslProvider = null;
	}

	TcpClientConfig(TcpClientConfig parent) {
		super(parent);
		this.sslProvider = parent.sslProvider;
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
		return TcpResources.get();
	}

	static SslProvider defaultSslProvider() {
		SslProvider sslProvider = CACHED_DEFAULT_SSL_PROVIDER;

		if (sslProvider == null) {
			sslProvider = SslProvider.create(ssl -> ssl.sslContext(SslContextBuilder.forClient())
			                                           .defaultConfiguration(SslProvider.DefaultConfigurationType.TCP));
			CACHED_DEFAULT_SSL_PROVIDER = sslProvider;
		}
		return sslProvider;
	}

	static SslProvider CACHED_DEFAULT_SSL_PROVIDER;

	static final String          CLIENT_METRICS_NAME = "reactor.netty.tcp.client";
	static final LoggingHandler  LOGGING_HANDLER     = new LoggingHandler(TcpClient.class);
	static final TcpClientConfig DEFAULT             = new TcpClientConfig(ConnectionProvider.newConnection());
}
