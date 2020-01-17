package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import reactor.netty.ConnectionObserver;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.LoopResources;
import reactor.util.annotation.Nullable;

/**
 * A basic configuration holder.
 *
 * @author Stephane Maldini
 */
public abstract class TransportConfig {

	/**
	 * Return the read-only default channel attributes
	 *
	 * @return the read-only default channel attributes
	 */
	public final Map<AttributeKey<?>, ?> attributes() {
		if (attrs.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(attrs);
	}

	/**
	 * Return the associated {@link ChannelOperations.OnSetup}, config implementations might override this
	 *
	 * @return the associated {@link ChannelOperations.OnSetup}
	 */
	public ChannelOperations.OnSetup channelOperationsProvider() {
		return DEFAULT_OPS;
	}

	/**
	 * Return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 *
	 * @return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()}
	 */
	public final ConnectionObserver connectionObserver() {
		return observer;
	}

	/**
	 * Return True if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 *
	 * @return True if prefer native event loop and channel factory (e.g. epoll or kqueue)
	 */
	public final boolean isPreferNative() {
		return this.preferNative;
	}

	/**
	 * Return the local {@link SocketAddress} that will be bound or empty
	 *
	 * @return the {@link SocketAddress}
	 */
	@Nullable
	public final SocketAddress localAddress() {
		return this.localAddress;
	}

	/**
	 * Return the configured {@link LoopResources} or null
	 *
	 * @return the configured  {@link LoopResources} or null
	 */
	@Nullable
	public final LoopResources loopResources() {
		return this.loopResources;
	}

	/**
	 * Return the channel type this configuration is associated with, for instance a {@link io.netty.channel.unix.DomainSocketChannel} or {@link io.netty.channel.socket.nio.NioDatagramChannel}
	 *
	 * @return the default {@link LoopResources} for this transport
	 */
	public final Class<? extends Channel> channelType() {
		return channelType;
	}

	/**
	 * Return the configured metrics category {@link String} or empty
	 *
	 * @return the configured metrics category {@link String} or empty
	 */
	@Nullable
	public final String metricsCategory() {
		return  this.metricsCategory;
	}

	/**
	 * Return the read-only {@link ChannelOption} map
	 *
	 * @return the read-only {@link ChannelOption} map
	 */
	public final Map<ChannelOption<?>, ?> options() {
		if (options.isEmpty()) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(options);
	}



	// Protected/Package private write API

	LoopResources            loopResources;
	Map<ChannelOption<?>, ?> options;
	Map<AttributeKey<?>, ?>  attrs;
	boolean                  preferNative;
	ConnectionObserver       observer;
	SocketAddress            localAddress;
	String                   metricsCategory;
	LoggingHandler           loggingHandler;
	Class<? extends Channel> channelType;

	/**
	 * Default TransportConfig
	 */
	TransportConfig() {
		this.observer = ConnectionObserver.emptyListener();
		this.options = Collections.emptyMap();
		this.attrs = Collections.emptyMap();
	}

	/**
	 * Default TransportConfig with options
	 */
	TransportConfig(Map<ChannelOption<?>, ?> options) {
		this.options = options;
		this.observer = ConnectionObserver.emptyListener();
		this.attrs = Collections.emptyMap();
		this.preferNative = LoopResources.DEFAULT_NATIVE;
	}

	/**
	 * Create TransportConfig from an existing one
	 */
	protected TransportConfig(TransportConfig parent) {
		this.loopResources = parent.loopResources;
		this.localAddress = parent.localAddress;
		this.preferNative = parent.preferNative;
		this.attrs = parent.attrs;
		this.options = parent.options;
		this.metricsCategory = parent.metricsCategory;
		this.observer = parent.observer;
		this.loggingHandler = parent.loggingHandler;
	}

	/**
	 * Return the default {@link LoopResources} for this transport
	 *
	 * @return the default {@link LoopResources} for this transport
	 */
	protected abstract LoopResources defaultLoopResources();

	/**
	 * Return the default Metrics category name for this transport
	 *
	 * @return the default Metrics category name for this transport
	 */
	protected abstract String defaultMetricsCategory();

	/**
	 * Return the default {@link LoggingHandler} to wiretap this transport
	 *
	 * @return the default {@link LoggingHandler} to wiretap this transport
	 */
	protected abstract LoggingHandler defaultLoggingHandler();

	/**
	 * Add or remove values to a map in an immutable way by returning a new map instance.
	 *
	 * @param parentMap the container map to update
	 * @param key the key to update
	 * @param value the new value or null to remove an existing key
	 * @param <K> key type to add
	 * @param <V> value to add
	 *
	 * @return a new instance of the map
	 */
	@SuppressWarnings("unchecked")
	protected static <K, V> Map<K, V> updateMap(Map parentMap, Object key, @Nullable Object value) {
		Objects.requireNonNull(key, "key");
		if (parentMap.isEmpty()) {
			return value == null ? parentMap : Collections.singletonMap((K)key, (V)value);
		}
		else {
			Map<K, V> attrs = new HashMap<>(parentMap.size() + 1);
			attrs.putAll((Map<K, V>)parentMap);
			if (value == null) {
				attrs.remove(key);
			}
			else {
				attrs.put((K)key, (V)value);
			}
			return attrs;
		}
	}

	static final ChannelOperations.OnSetup DEFAULT_OPS = (ch, c, msg) -> new ChannelOperations<>(ch, c);
}
