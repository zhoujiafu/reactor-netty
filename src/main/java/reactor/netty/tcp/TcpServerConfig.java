package reactor.netty.tcp;

import java.util.Collections;
import java.util.Map;

import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportServerConfig;
import reactor.util.annotation.Nullable;

/**
 * Encapsulate all necessary configuration for client TCP transport. The public API is read-only.
 */
public final class TcpServerConfig extends TransportServerConfig<TcpServerConfig, DisposableServer> {

	/**
	 * Return the default {@link TcpServerConfig} configuration
	 *
	 * @return the default {@link TcpServerConfig} configuration
	 */
	public static TcpServerConfig defaultServer() {
		return DEFAULT;
	}

	/**
	 * Returns true if that {@link TcpServer} secured via SSL transport
	 *
	 * @return true if that {@link TcpServer} secured via SSL transport
	 */
	public final boolean isSecure(){
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

	SslProvider                        sslProvider;

	TcpServerConfig() {
		super(DEFAULT_SELECTOR_OPTIONS, DEFAULT_CHILD_OPTIONS);
	}

	TcpServerConfig(TcpServerConfig parent) {
		super(parent);
		this.sslProvider = parent.sslProvider;
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
		return TcpResources.get();
	}

	static final String          SERVER_METRICS_NAME = "reactor.netty.tcp.server";
	static final LoggingHandler  LOGGING_HANDLER     = new LoggingHandler(TcpServer.class);
	static final TcpServerConfig DEFAULT             = new TcpServerConfig();

	static final Map<ChannelOption<?>, Object> DEFAULT_SELECTOR_OPTIONS;
	static final Map<ChannelOption<?>, Object> DEFAULT_CHILD_OPTIONS;

	static {
		DEFAULT_SELECTOR_OPTIONS = Collections.singletonMap(ChannelOption.SO_REUSEADDR, true);
		DEFAULT_CHILD_OPTIONS = Collections.singletonMap(ChannelOption.TCP_NODELAY, true);
	}
}
