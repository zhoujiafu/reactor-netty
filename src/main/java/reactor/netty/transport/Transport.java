package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Objects;

import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import reactor.netty.ConnectionObserver;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
import reactor.util.Metrics;
import reactor.util.annotation.Nullable;

/**
 * An immutable transport builder for clients and servers.
 * 
 * @param <T> Transport implementation
 * @param <T> Transport Config implementation
 * @author Stephane Maldini
 */
public abstract class Transport<T extends Transport<T, C>, C extends TransportConfig> {


	/**
	 * Update the given option key or remove if value is {@literal null}
	 *
	 * @param attributeKey the {@link AttributeKey} key
	 * @param value the {@link AttributeKey} value
	 * @param <A> attribute type
	 *
	 * @return a new {@link Transport} reference
	 */
	public final <A> T attr(AttributeKey<A> attributeKey, @Nullable A value) {
		T dup = duplicate();
		dup.configuration().attrs = TransportConfig.updateMap(configuration().attrs, attributeKey, value);
		return dup;
	}

	/**
	 * Return a {@link TransportConfig}
	 *
	 * @return a new {@link Transport} reference
	 */
	public abstract C configuration();

	/**
	 * Set new localAddress
	 *
	 * @param localAddress new localAddress
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T localAddress(SocketAddress localAddress) {
		T dup = duplicate();
		dup.configuration().localAddress = Objects.requireNonNull(localAddress, "localAddress");
		return dup;
	}

	/**
	 * Specifies whether the metrics are enabled on the {@link TcpClient},
	 * assuming Micrometer is on the classpath.
	 * The metrics name will be derived from {@link TransportConfig#defaultMetricsCategory}.
	 *
	 * @param metricsEnabled if true enables the metrics on the client.
	 * @return a new {@link Transport} reference
	 */
	public final T metrics(boolean metricsEnabled) {
		return metrics(metricsEnabled, configuration().defaultMetricsCategory());
	}

	/**
	 * Specifies whether the metrics are enabled on the {@link TcpClient},
	 * assuming Micrometer is on the classpath.
	 *
	 * @param metricsEnabled if true enables the metrics on the client.
	 * @param metricsCategory the name to be used for the metrics
	 * @return a new {@link Transport} reference
	 */
	public final T metrics(boolean metricsEnabled, String metricsCategory) {
		Objects.requireNonNull(metricsCategory, "metricsCategory");
		if (!Metrics.isInstrumentationAvailable()) {
			throw new UnsupportedOperationException(
					"To enable metrics, you must add the dependency `io.micrometer:micrometer-core`" +
							" to the class path first");
		}
		if (metricsEnabled) {
			T dup = duplicate();
			dup.configuration().metricsCategory = metricsCategory;
			return dup;
		}
		if (configuration().metricsCategory() != null) {
			T dup = duplicate();
			dup.configuration().metricsCategory = null;
			return dup;
		}
		@SuppressWarnings("unchecked")
		T dup = (T)this;
		return dup;
	}

	/**
	 * Update the given option key or remove if value is {@literal null}
	 *
	 * @param optionKey the {@link ChannelOption} key
	 * @param value the {@link ChannelOption} value or null
	 * @param <O> the value of the type
	 *
	 * @return a new {@link Transport} reference
	 */
	public final <O> T option(ChannelOption<O> optionKey, @Nullable O value) {
		T dup = duplicate();
		//FIXME ignore optionKey == ChannelOption.AUTO_READ
		dup.configuration().options = TransportConfig.updateMap(configuration().options, optionKey, value);
		return dup;
	}

	/**
	 * Set or add the given {@link ConnectionObserver}
	 *
	 * @param observer the {@link ConnectionObserver} addition
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T observe(ConnectionObserver observer) {
		T dup = duplicate();
		dup.configuration().observer = observer.then(observer);
		return dup;
	}

	/**
	 * Run IO loops on the given {@link EventLoopGroup}.
	 *
	 * @param eventLoopGroup an eventLoopGroup to share
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T runOn(EventLoopGroup eventLoopGroup) {
		Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
		return runOn(preferNative -> eventLoopGroup);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the
	 * {@link LoopResources} container. Will prefer native (epoll/kqueue) implementation if
	 * available unless the environment property {@code reactor.netty.native} is set
	 * to {@code false}.
	 *
	 * @param channelResources a {@link LoopResources} accepting native runtime expectation and
	 * returning an eventLoopGroup
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T runOn(LoopResources channelResources) {
		return runOn(channelResources, LoopResources.DEFAULT_NATIVE);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources} container.
	 *
	 * @param loopResources new loop resources
	 * @param preferNative should prefer running on epoll, kqueue or similar instead of java NIO
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T runOn(LoopResources loopResources, boolean preferNative) {
		T dup = duplicate();
		TransportConfig c = dup.configuration();
		c.loopResources = Objects.requireNonNull(loopResources, "loopResources");
		c.preferNative = preferNative;
		return dup;
	}

	/**
	 * Apply or remove a wire logger configuration using {@link TcpClient} category
	 * and {@code DEBUG} logger level
	 *
	 * @param enable Specifies whether the wire logger configuration will be added to
	 *               the pipeline
	 * @return a new {@link Transport} reference
	 */
	public final T wiretap(boolean enable) {
		if (enable) {
			T dup = duplicate();
			configuration().loggingHandler = configuration().defaultLoggingHandler();
			return dup;
		}
		else if (configuration().loggingHandler != null) {
			T dup = duplicate();
			configuration().loggingHandler = null;
			return dup;
		}
		else {
			@SuppressWarnings("unchecked")
			T dup = (T) this;
			return dup;
		}
	}

	/**
	 * Apply a wire logger configuration using the specified category
	 * and {@code DEBUG} logger level
	 *
	 * @param category the logger category
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T wiretap(String category) {
		return wiretap(category, LogLevel.DEBUG);
	}

	/**
	 * Apply a wire logger configuration using the specified category
	 * and logger level
	 *
	 * @param category the logger category
	 * @param level the logger level
	 *
	 * @return a new {@link Transport} reference
	 */
	public final T wiretap(String category, LogLevel level) {
		Objects.requireNonNull(category, "category");
		Objects.requireNonNull(level, "level");
		T dup = duplicate();
		dup.configuration().loggingHandler = new LoggingHandler(category, level);
		return dup;
	}

	/**
	 * Return a new {@link Transport} inheriting the current configuration
	 *
	 * @return a new {@link Transport} inheriting the current configuration
	 */
	protected abstract T duplicate();
}
