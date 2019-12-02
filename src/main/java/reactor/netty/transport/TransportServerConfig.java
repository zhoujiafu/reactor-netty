package reactor.netty.transport;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import io.netty.channel.ChannelOption;
import io.netty.util.AttributeKey;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.DisposableServer;

/**
 * Encapsulate all necessary configuration for server TCP transport. The public API is read-only.
 *
 * @param <CONF> Configuration implementation
 * @param <SERV> Connection implementation
 *
 */
public abstract class TransportServerConfig<CONF extends TransportConfig, SERV extends DisposableServer> extends TransportConfig {

	/**
	 * Return the read-only default channel attributes for each remote connection
	 *
	 * @return the read-only default channel attributes for each remote connection
	 */
	public final Map<AttributeKey<?>, Object> childAttributes() {
		if (childAttrs == null) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(childAttrs);
	}

	/**
	 * Return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()} for each remote connection
	 *
	 * @return the configured {@link ConnectionObserver} if any or {@link ConnectionObserver#emptyListener()} for each remote connection
	 */
	public final ConnectionObserver childObserver() {
		return childObserver;
	}

	/**
	 * Return the read-only {@link ChannelOption} map for each remote connection
	 *
	 * @return the read-only {@link ChannelOption} map for each remote connection
	 */
	public final Map<ChannelOption<?>, Object> childOptions() {
		if (childOptions == null) {
			return Collections.emptyMap();
		}
		return Collections.unmodifiableMap(childOptions);
	}

	// Package private creators

	Map<AttributeKey<?>, ?>      childAttrs;
	Map<ChannelOption<?>, ?>     childOptions;
	ConnectionObserver           childObserver;
	Consumer<? super CONF>       doOnBind;
	Consumer<? super SERV>       doOnBound;
	Consumer<? super SERV>       doOnUnbound;
	Consumer<? super Connection> doOnConnection;

	/**
	 * Default server config with no options
	 */
	protected TransportServerConfig() {
		this(Collections.emptyMap(), Collections.emptyMap());
	}

	/**
	 * Default TransportServerConfig with options
	 *
	 * @param options default options for the selector
	 * @param childOptions default options for each connected channel
	 */
	protected TransportServerConfig(Map<ChannelOption<?>, ?> options, Map<ChannelOption<?>, ?> childOptions) {
		super(options);
		this.childObserver = ConnectionObserver.emptyListener();
		this.childAttrs = Collections.emptyMap();
		this.childOptions = childOptions;

	}

	protected TransportServerConfig(TransportServerConfig<CONF, SERV> parent) {
		super(parent);
		this.childAttrs = parent.childAttrs;
		this.childOptions = parent.childOptions;
		this.doOnBind = parent.doOnBind;
		this.doOnBound = parent.doOnBound;
		this.childObserver = parent.childObserver;
		this.doOnUnbound = parent.doOnUnbound;
		this.doOnConnection = parent.doOnConnection;
	}
}
