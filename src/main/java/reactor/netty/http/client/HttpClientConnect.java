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

package reactor.netty.http.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2EventAdapter;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2MultiplexCodecBuilder;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyOutbound;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.AbortedException;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.http.HttpResources;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.InetSocketAddressUtil;
import reactor.netty.tcp.ProxyProvider;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpClient;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http2.Http2Error.INTERNAL_ERROR;
import static io.netty.handler.codec.http2.Http2Error.PROTOCOL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.connectionError;
import static reactor.netty.ReactorNetty.format;

/**
 * @author Stephane Maldini
 */
final class HttpClientConnect extends HttpClient {

	final HttpTcpClient defaultClient;

	HttpClientConnect() {
		this(DEFAULT_TCP_CLIENT);
	}

	HttpClientConnect(TcpClient defaultClient) {
		Objects.requireNonNull(defaultClient, "tcpClient");
		this.defaultClient = new HttpTcpClient(defaultClient);
	}

	@Override
	protected TcpClient tcpConfiguration() {
		return defaultClient;
	}

	static final class HttpTcpClient extends TcpClient {

		final TcpClient defaultClient;

		HttpTcpClient(TcpClient defaultClient) {
			this.defaultClient = defaultClient;
		}

		@Override
		public Mono<? extends Connection> connect(Bootstrap b) {
			SslProvider ssl = SslProvider.findSslSupport(b);

			if (b.config()
			     .group() == null) {

				LoopResources loops = HttpResources.get();

				SslContext sslContext = ssl != null ? ssl.getSslContext() : null;

				boolean useNative =
						LoopResources.DEFAULT_NATIVE && !(sslContext instanceof JdkSslContext);

				EventLoopGroup elg = loops.onClient(useNative);

				Integer maxConnections = (Integer) b.config().attrs().get(AttributeKey.valueOf("maxConnections"));

				if (maxConnections != null && maxConnections != -1 && elg instanceof Supplier) {
					EventLoopGroup delegate = (EventLoopGroup) ((Supplier) elg).get();
					b.group(delegate)
					 .channel(loops.onChannel(delegate));
				}
				else {
					b.group(elg)
					 .channel(loops.onChannel(elg));
				}
			}

			HttpClientConfiguration conf = HttpClientConfiguration.getAndClean(b);

			if (ssl != null) {
				if (ssl.getDefaultConfigurationType() == null) {
					switch (conf.protocols) {
						case HttpClientConfiguration.h11:
							ssl = SslProvider.updateDefaultConfiguration(ssl, SslProvider.DefaultConfigurationType.TCP);
							break;
						case HttpClientConfiguration.h2:
							ssl = SslProvider.updateDefaultConfiguration(ssl, SslProvider.DefaultConfigurationType.H2);
							break;
					}
				}
				SslProvider.setBootstrap(b, ssl);
			}

			SslProvider defaultSsl = ssl;

			if (conf.deferredConf != null) {
				return Mono.fromCallable(() -> new HttpClientConfiguration(conf))
				           .transform(conf.deferredConf)
				           .flatMap(c -> new MonoHttpConnect(b, c, defaultClient, defaultSsl));
			}

			return new MonoHttpConnect(b, conf, defaultClient, defaultSsl);
		}

		@Override
		public Bootstrap configure() {
			return defaultClient.configure();
		}

		@Nullable
		@Override
		public ProxyProvider proxyProvider() {
			return defaultClient.proxyProvider();
		}

		@Nullable
		@Override
		public SslProvider sslProvider() {
			return defaultClient.sslProvider();
		}
	}


	static final class MonoHttpConnect extends Mono<Connection> {

		final Bootstrap               bootstrap;
		final HttpClientConfiguration configuration;
		final TcpClient               tcpClient;
		final SslProvider             sslProvider;
		final ProxyProvider           proxyProvider;

		MonoHttpConnect(Bootstrap bootstrap,
				HttpClientConfiguration configuration,
				TcpClient tcpClient,
				@Nullable SslProvider               sslProvider) {
			this.bootstrap = bootstrap;
			this.configuration = configuration;
			this.sslProvider = sslProvider;
			this.tcpClient = tcpClient;
			this.proxyProvider = ProxyProvider.findProxySupport(bootstrap);
		}

		@Override
		public void subscribe(CoreSubscriber<? super Connection> actual) {
			final Bootstrap b = bootstrap.clone();

			BootstrapHandlers.channelOperationFactory(b);

			HttpClientHandler handler = new HttpClientHandler(configuration, b.config()
			                                                                  .remoteAddress(), sslProvider, proxyProvider);

			b.remoteAddress(handler);

			if (sslProvider != null) {
				if ((configuration.protocols & HttpClientConfiguration.h2c) == HttpClientConfiguration.h2c) {
					Operators.error(actual, new IllegalArgumentException("Configured" +
							" H2 Clear-Text protocol " +
							"with TLS. Use the non clear-text h2 protocol via " +
							"HttpClient#protocol or disable TLS" +
							" via HttpClient#tcpConfiguration(tcp -> tcp.noSSL())"));
					return;
				}
//				if ((configuration.protocols & HttpClientConfiguration.h11orH2) == HttpClientConfiguration.h11orH2) {
//					return BootstrapHandlers.updateConfiguration(b,
//							NettyPipeline.HttpInitializer,
//							new Http1OrH2Initializer(conf.decoder.maxInitialLineLength(),
//									conf.decoder.maxHeaderSize(),
//									conf.decoder.maxChunkSize(),
//									conf.decoder.validateHeaders(),
//									conf.decoder.initialBufferSize(),
//									conf.minCompressionSize,
//									compressPredicate(conf.compressPredicate, conf.minCompressionSize),
//									conf.forwarded,
//									conf.cookieEncoder,
//									conf.cookieDecoder));
//				}
				if ((configuration.protocols & HttpClientConfiguration.h11) == HttpClientConfiguration.h11) {
					BootstrapHandlers.updateConfiguration(b, NettyPipeline.HttpInitializer,
							new Http1Initializer(handler, configuration.protocols));
				}
				else if ((configuration.protocols & HttpClientConfiguration.h2) == HttpClientConfiguration.h2) {
					handler.defaultHeaders.add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), HttpScheme.HTTPS);
					BootstrapHandlers.updateConfiguration(b,
							NettyPipeline.HttpInitializer,
							new H2Initializer(handler));
				}
			}
			else {
				if ((configuration.protocols & HttpClientConfiguration.h2) == HttpClientConfiguration.h2) {
					Operators.error(actual, new IllegalArgumentException(
							"Configured H2 protocol without TLS. Use" + " a clear-text " + "h2 protocol via HttpClient#protocol or configure TLS" + " via HttpClient#secure"));
					return;
				}
//				if ((configuration.protocols & HttpClientConfiguration.h11orH2c) == HttpClientConfiguration.h11orH2c) {
//					BootstrapHandlers.updateConfiguration(b,
//							NettyPipeline.HttpInitializer,
//							new Http1OrH2CleartextInitializer(configuration.decoder.maxInitialLineLength,
//									configuration.decoder.maxHeaderSize,
//									configuration.decoder.maxChunkSize,
//									configuration.decoder.validateHeaders,
//									configuration.decoder.initialBufferSize,
//									configuration.minCompressionSize,
//									compressPredicate(configuration.compressPredicate, configuration.minCompressionSize),
//									configuration.forwarded));
//					return;
//				}
				if ((configuration.protocols & HttpClientConfiguration.h11) == HttpClientConfiguration.h11) {
					BootstrapHandlers.updateConfiguration(b,
							NettyPipeline.HttpInitializer,
							new Http1Initializer(handler, configuration.protocols));
//					return;
				}
//				if ((configuration.protocols & HttpClientConfiguration.h2c) == HttpClientConfiguration.h2c) {
//					BootstrapHandlers.updateConfiguration(b,
//							NettyPipeline.HttpInitializer,
//							new H2CleartextInitializer(
//									conf.decoder.validateHeaders,
//									conf.minCompressionSize,
//									compressPredicate(conf.compressPredicate, conf.minCompressionSize),
//									conf.forwarded));
//					return;
//				}
			}

//			Operators.error(actual, new IllegalArgumentException("An unknown " +
//					"HttpClient#protocol " + "configuration has been provided: "+String.format("0x%x", configuration.protocols)));

			Mono.<Connection>create(sink -> {
				Bootstrap finalBootstrap;
				//append secure handler if needed
				if (handler.activeURI.isSecure()) {
					if (sslProvider == null) {
						if ((configuration.protocols & HttpClientConfiguration.h2c) == HttpClientConfiguration.h2c) {
							sink.error(new IllegalArgumentException("Configured H2 " +
									"Clear-Text" + " protocol" + " without TLS while " +
									"trying to redirect to a secure address."));
							return;
						}
						//should not need to handle H2 case already checked outside of
						// this callback
						finalBootstrap = SslProvider.setBootstrap(b.clone(),
								HttpClientSecure.DEFAULT_HTTP_SSL_PROVIDER);
					}
					else {
						finalBootstrap = b.clone();
					}
				}
				else {
					if (sslProvider != null) {
						finalBootstrap = SslProvider.removeSslSupport(b.clone());
					}
					else {
						finalBootstrap = b.clone();
					}
				}

				BootstrapHandlers.connectionObserver(finalBootstrap,
						new HttpObserver(sink, handler, BootstrapHandlers.connectionObserver(finalBootstrap)));

				tcpClient.connect(finalBootstrap)
				         .subscribe(new TcpClientSubscriber(sink));

			}).retry(handler)
			  .subscribe(actual);
		}

		final static class TcpClientSubscriber implements CoreSubscriber<Connection> {

			final MonoSink<Connection> sink;

			TcpClientSubscriber(MonoSink<Connection> sink) {
				this.sink = sink;
			}

			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Connection connection) {
				sink.onCancel(connection);
			}

			@Override
			public void onError(Throwable throwable) {
				sink.error(throwable);
			}

			@Override
			public void onComplete() {

			}

			@Override
			public Context currentContext() {
				return sink.currentContext();
			}
		}
	}

	final static class HttpObserver implements ConnectionObserver {

		final MonoSink<Connection> sink;
		final HttpClientHandler handler;
		final ConnectionObserver delegate;

		HttpObserver(MonoSink<Connection> sink, HttpClientHandler handler, ConnectionObserver delegate) {
			this.sink = sink;
			this.handler = handler;
			this.delegate = delegate;
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		public void onUncaughtException(Connection connection, Throwable error) {
			if (error instanceof RedirectClientException) {
				if (log.isDebugEnabled()) {
					log.debug(format(connection.channel(), "The request will be redirected"));
				}
			}
			else if (AbortedException.isConnectionReset(error)) {
				if (log.isDebugEnabled()) {
					log.debug(format(connection.channel(), "The connection observed an error, " +
							"the request will be retried"), error);
				}
			}
			else if (log.isWarnEnabled()) {
				log.warn(format(connection.channel(), "The connection observed an error"), error);
			}

			delegate.onUncaughtException(connection, error);

			sink.error(error);
		}

		@Override
		public void onStateChange(Connection connection, State newState) {
			if (newState == HttpClientState.RESPONSE_RECEIVED) {
				sink.success(connection);
				delegate.onStateChange(connection, newState);
				return;
			}

			if (newState == State.CONFIGURED) {
				return;
			}

			if (newState == State.CONNECTED || newState == State.ACQUIRED) {
				ConnectionObserver l;
				if (connection instanceof ConnectionObserver) {
					l = (ConnectionObserver)connection;
				}
				else {
					l = this;
				}
				HttpClientOperations ops = handler.h2 ?
						new HttpToH2Operations(connection, l, handler.cookieEncoder, handler.cookieDecoder) :
						new HttpClientOperations(connection, l, handler.cookieEncoder, handler.cookieDecoder);

				ops.bind();

				handler.channel(ops);

				delegate.onStateChange(ops, State.CONFIGURED);

				if (log.isDebugEnabled()) {
					log.debug(format(connection.channel(), "Handler is being applied: {}"), handler);
				}

				Mono.fromDirect(handler.requestWithBody(ops))
				    .subscribe(ops.disposeSubscriber());

				return;
			}

			delegate.onStateChange(connection, newState);
		}
	}

	static final class HttpClientHandler extends SocketAddress
			implements Predicate<Throwable>, Supplier<SocketAddress> {

		final HttpMethod         method;
		final HttpHeaders        defaultHeaders;
		final BiFunction<? super HttpClientRequest, ? super NettyOutbound, ? extends Publisher<Void>>
		                         handler;
		final boolean            compress;
		final UriEndpointFactory uriEndpointFactory;
		final String             websocketProtocols;
		final int                maxFramePayloadLength;

		final ClientCookieEncoder cookieEncoder;
		final ClientCookieDecoder cookieDecoder;

		final BiPredicate<HttpClientRequest, HttpClientResponse> followRedirectPredicate;

		final HttpResponseDecoderSpec decoder;

		final ProxyProvider proxyProvider;

		final boolean h2;

		volatile UriEndpoint        activeURI;
		volatile Supplier<String>[] redirectedFrom;
		volatile boolean retried;

		@SuppressWarnings("unchecked")
		HttpClientHandler(HttpClientConfiguration configuration, @Nullable SocketAddress address,
				@Nullable SslProvider sslProvider, @Nullable ProxyProvider proxyProvider) {
			this.method = configuration.method;
			this.compress = configuration.acceptGzip;
			this.followRedirectPredicate = configuration.followRedirectPredicate;
			this.cookieEncoder = configuration.cookieEncoder;
			this.cookieDecoder = configuration.cookieDecoder;
			this.decoder = configuration.decoder;
			this.proxyProvider = proxyProvider;
			this.h2 = (configuration.protocols & (HttpClientConfiguration.h2 | HttpClientConfiguration.h2c)) != 0;


			HttpHeaders defaultHeaders = configuration.headers;
			if (compress) {
				if (defaultHeaders == null) {
					this.defaultHeaders = new DefaultHttpHeaders();
				}
				else {
					this.defaultHeaders = defaultHeaders;
				}
				this.defaultHeaders.set(HttpHeaderNames.ACCEPT_ENCODING,
				                        HttpHeaderValues.GZIP);
			}
			//if h2 will need to add scheme headers
			else if (defaultHeaders == null && h2){
				this.defaultHeaders = new DefaultHttpHeaders();
			}
			else {
				this.defaultHeaders = defaultHeaders;
			}

			String baseUrl = configuration.baseUrl;

			String uri = configuration.uri;

			uri = uri == null ? "/" : uri;

			if (baseUrl != null && uri.startsWith("/")) {
				if (baseUrl.endsWith("/")) {
					baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
				}
				uri = baseUrl + uri;
			}

			Supplier<SocketAddress> addressSupplier;
			if (address instanceof Supplier) {
				addressSupplier = (Supplier<SocketAddress>) address;
			}
			else {
				addressSupplier = () -> address;
			}

			this.uriEndpointFactory =
					new UriEndpointFactory(addressSupplier, sslProvider != null, URI_ADDRESS_MAPPER);

			this.websocketProtocols = configuration.websocketSubprotocols;
			this.maxFramePayloadLength = configuration.websocketMaxFramePayloadLength;
			this.handler = configuration.body;
			this.activeURI = uriEndpointFactory.createUriEndpoint(uri, configuration.websocketSubprotocols != null);
		}

		@Override
		public SocketAddress get() {
			SocketAddress address = activeURI.getRemoteAddress();
			if (proxyProvider != null && !proxyProvider.shouldProxy(address) &&
					address instanceof InetSocketAddress) {
				address = InetSocketAddressUtil.replaceWithResolved((InetSocketAddress) address);
			}

			return address;
		}

		Publisher<Void> requestWithBody(HttpClientOperations ch) {
			try {
				UriEndpoint uri = activeURI;
				HttpHeaders headers = ch.getNettyRequest()
				                        .setUri(uri.getPathAndQuery())
				                        .setMethod(method)
				                        .setProtocolVersion(HttpVersion.HTTP_1_1)
				                        .headers();

				if (defaultHeaders != null) {
					headers.set(defaultHeaders);
				}

				if (!headers.contains(HttpHeaderNames.USER_AGENT)) {
					headers.set(HttpHeaderNames.USER_AGENT, USER_AGENT);
				}

				SocketAddress remoteAddress = uri.getRemoteAddress();
				if (!headers.contains(HttpHeaderNames.HOST) && remoteAddress instanceof InetSocketAddress) {
					headers.set(HttpHeaderNames.HOST,
					            resolveHostHeaderValue((InetSocketAddress) remoteAddress));
				}

				if (!headers.contains(HttpHeaderNames.ACCEPT)) {
					headers.set(HttpHeaderNames.ACCEPT, ALL);
				}

				ch.followRedirectPredicate(followRedirectPredicate);

				if (!Objects.equals(method, HttpMethod.GET) &&
						!Objects.equals(method, HttpMethod.HEAD) &&
						!Objects.equals(method, HttpMethod.DELETE) &&
						!headers.contains(HttpHeaderNames.CONTENT_LENGTH)) {
					ch.chunkedTransfer(true);
				}

				if (handler != null) {
					if (websocketProtocols != null) {
						return Mono.fromRunnable(() -> ch.withWebsocketSupport(websocketProtocols, maxFramePayloadLength, compress))
						           .thenEmpty(Mono.fromRunnable(() -> Flux.concat(handler.apply(ch, ch))));
					}
					else {
						return handler.apply(ch, ch);
					}
				}
				else {
					if (websocketProtocols != null) {
						return Mono.fromRunnable(() -> ch.withWebsocketSupport(websocketProtocols, maxFramePayloadLength, compress));
					}
					else {
						return ch.send();
					}
				}
			}
			catch (Throwable t) {
				return Mono.error(t);
			}
		}

		static String resolveHostHeaderValue(@Nullable InetSocketAddress remoteAddress) {
			if (remoteAddress != null) {
				String host = HttpUtil.formatHostnameForHttp(remoteAddress);
				int port = remoteAddress.getPort();
				if (port != 80 && port != 443) {
					host = host + ':' + port;
				}
				return host;
			}
			else {
				return "localhost";
			}
		}

		void redirect(String to) {
			Supplier<String>[] redirectedFrom = this.redirectedFrom;
			UriEndpoint from = activeURI;
			if (to.startsWith("/")) {
				activeURI = uriEndpointFactory.createUriEndpoint(to, from.isWs(),
						() -> URI_ADDRESS_MAPPER.apply(from.host, from.port));
			}
			else {
				activeURI = uriEndpointFactory.createUriEndpoint(to, from.isWs());
			}
			this.redirectedFrom = addToRedirectedFromArray(redirectedFrom, from);
		}

		@SuppressWarnings("unchecked")
		static Supplier<String>[] addToRedirectedFromArray(@Nullable Supplier<String>[] redirectedFrom,
				UriEndpoint from) {
			Supplier<String> fromUrlSupplier = from::toExternalForm;
			if (redirectedFrom == null) {
				return new Supplier[]{fromUrlSupplier};
			}
			else {
				Supplier<String>[] newRedirectedFrom =
						new Supplier[redirectedFrom.length + 1];
				System.arraycopy(redirectedFrom,
						0,
						newRedirectedFrom,
						0,
						redirectedFrom.length);
				newRedirectedFrom[redirectedFrom.length] = fromUrlSupplier;
				return newRedirectedFrom;
			}
		}

		void channel(HttpClientOperations ops) {
			Supplier<String>[] redirectedFrom = this.redirectedFrom;
			if (redirectedFrom != null) {
				ops.redirectedFrom = redirectedFrom;
			}
		}

		@Override
		public boolean test(Throwable throwable) {
			if (throwable instanceof RedirectClientException) {
				RedirectClientException re = (RedirectClientException) throwable;
				redirect(re.location);
				return true;
			}
			if (AbortedException.isConnectionReset(throwable) && !retried) {
				retried = true;
				redirect(activeURI.toString());
				return true;
			}
			return false;
		}

		@Override
		public String toString() {
			return "{" + "uri=" + activeURI + ", method=" + method + '}';
		}
	}


	static final class Http1Initializer
			implements BiConsumer<ConnectionObserver, Channel>  {

		final HttpClientHandler handler;
		final int protocols;

		Http1Initializer(HttpClientHandler handler, int protocols) {
			this.handler = handler;
			this.protocols = protocols;
		}

		@Override
		public void accept(ConnectionObserver listener, Channel channel) {
			channel.pipeline()
			       .addLast(NettyPipeline.HttpCodec,
			                new HttpClientCodec(handler.decoder.maxInitialLineLength(),
			                                    handler.decoder.maxHeaderSize(),
			                                    handler.decoder.maxChunkSize(),
			                                    handler.decoder.failOnMissingResponse,
			                                    handler.decoder.validateHeaders(),
			                                    handler.decoder.initialBufferSize(),
			                                    handler.decoder.parseHttpAfterConnectRequest));

			if (handler.compress) {
				channel.pipeline()
				       .addAfter(NettyPipeline.HttpCodec,
						       NettyPipeline.HttpDecompressor,
						       new HttpContentDecompressor());
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Http1Initializer that = (Http1Initializer) o;
			return handler.compress == that.handler.compress &&
					protocols == that.protocols;
		}

		@Override
		public int hashCode() {
			return Objects.hash(handler.compress, protocols);
		}
	}

//

//	@ChannelHandler.Sharable
//	static final class HttpClientInitializer
//			extends ChannelInboundHandlerAdapter
//			implements BiConsumer<ConnectionObserver, Channel>, ChannelOperations
//			.OnSetup, GenericFutureListener<Future<Http2StreamChannel>> {
//		final HttpClientHandler handler;
//		final DirectProcessor<Void> upgraded;
//
//		HttpClientInitializer(HttpClientHandler handler) {
//			this.handler = handler;
//			this.upgraded = DirectProcessor.create();
//		}
//
//		@Override
//		public void operationComplete(Future<Http2StreamChannel> future) {
//			if (!future.isSuccess()) {
//				upgraded.onError(future.cause());
//			}
//		}
//
//		@Override
//		public void channelActive(ChannelHandlerContext ctx) {
//			ChannelOperations<?, ?> ops = ChannelOperations.get(ctx.channel());
//			if (ops != null) {
//				ops.listener().onStateChange(ops, ConnectionObserver.State.CONFIGURED);
//			}
//			ctx.fireChannelActive();
//		}
//
//		@Override
//		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
//			if(evt == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_SUCCESSFUL) {
//				//upgraded to h2, read the next streams/settings from server
//
//				ctx.channel()
//				   .read();
//
//				ctx.pipeline()
//				   .remove(this);
//			}
//			else if (evt == HttpClientUpgradeHandler.UpgradeEvent.UPGRADE_REJECTED) {
//				ctx.pipeline()
//				   .remove(this);
//
//
//				if (handler.compress) {
//					ctx.pipeline()
//					   .addLast(NettyPipeline.HttpDecompressor, new HttpContentDecompressor());
//				}
//			}
//			ctx.fireUserEventTriggered(evt);
//		}
//
//		@Override
//		public ChannelOperations<?, ?> create(Connection c, ConnectionObserver listener, @Nullable Object msg) {
//			return new HttpClientOperations(c, listener, handler.cookieEncoder, handler.cookieDecoder);
//		}
//
//		@Override
//		public void accept(ConnectionObserver listener, Channel channel) {
//			ChannelPipeline p = channel.pipeline();
//
//			if (p.get(NettyPipeline.SslHandler) != null) {
//				p.addLast(new Http1OrH2Codec(listener, this));
//			}
//			else {
//				HttpClientCodec httpClientCodec =
//						new HttpClientCodec(handler.decoder.maxInitialLineLength(),
//						                    handler.decoder.maxHeaderSize(),
//						                    handler.decoder.maxChunkSize(),
//						                    handler.decoder.failOnMissingResponse,
//						                    handler.decoder.validateHeaders(),
//						                    handler.decoder.initialBufferSize(),
//						                    handler.decoder.parseHttpAfterConnectRequest);
//
//				final Http2Connection connection = new DefaultHttp2Connection(false);
//				HttpToHttp2ConnectionHandlerBuilder h2HandlerBuilder = new
//						HttpToHttp2ConnectionHandlerBuilder()
//						.frameListener(new InboundHttp2ToHttpAdapterBuilder(connection)
//										.maxContentLength(65536)
//										.propagateSettings(true)
//										.build())
//						.connection(connection);
//
//				if (p.get(NettyPipeline.LoggingHandler) != null) {
//					h2HandlerBuilder.frameLogger(new Http2FrameLogger(LogLevel.DEBUG, HttpClient.class));
//				}
//
//				p.addLast(NettyPipeline.HttpCodec, httpClientCodec);
////				 .addLast(new HttpClientUpgradeHandler(httpClientCodec,
////				          new Http2ClientUpgradeCodec(h2HandlerBuilder.build()), 65536))
////				 .addLast(this);
////				ChannelOperations<?, ?> ops = HTTP_OPS.create(Connection.from(channel), listener,	null);
////				if (ops != null) {
////					ops.bind();
////					listener.onStateChange(ops, ConnectionObserver.State.CONFIGURED);
////				}
//
//				upgraded.onComplete();
//			}
//		}
//	}

//	static final class Http1OrH2Codec extends ApplicationProtocolNegotiationHandler {
//
//		final HttpClientInitializer parent;
//		final ConnectionObserver listener;
//
//		Http1OrH2Codec(ConnectionObserver listener, HttpClientInitializer parent) {
//			super(ApplicationProtocolNames.HTTP_1_1);
//			this.listener = listener;
//			this.parent = parent;
//		}
//
//		@Override
//		protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
//			ChannelPipeline p = ctx.pipeline();
//
//			if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
//				Http2MultiplexCodecBuilder http2MultiplexCodecBuilder =
//						Http2MultiplexCodecBuilder.forClient(new Http2StreamInitializer(listener, parent.handler.cookieEncoder, parent.handler.cookieDecoder))
//						                          .validateHeaders(parent.handler.decoder.validateHeaders())
//						                          .initialSettings(Http2Settings.defaultSettings());
//
//				if (p.get(NettyPipeline.LoggingHandler) != null) {
//					http2MultiplexCodecBuilder.frameLogger(new Http2FrameLogger(LogLevel.DEBUG, HttpClient.class));
//				}
//
//				p.addLast(http2MultiplexCodecBuilder.build());
//
//				openStream(ctx.channel(), listener, parent);
//
//				return;
//			}
//
//			if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
//				p.addBefore(NettyPipeline.ReactiveBridge, NettyPipeline.HttpCodec,
//						new HttpClientCodec(parent.handler.decoder.maxInitialLineLength(),
//						                    parent.handler.decoder.maxHeaderSize(),
//						                    parent.handler.decoder.maxChunkSize(),
//						                    parent.handler.decoder.failOnMissingResponse,
//						                    parent.handler.decoder.validateHeaders(),
//						                    parent.handler.decoder.initialBufferSize(),
//						                    parent.handler.decoder.parseHttpAfterConnectRequest));
//
//				if (parent.handler.compress) {
//					p.addAfter(NettyPipeline.HttpCodec,
//					           NettyPipeline.HttpDecompressor,
//					           new HttpContentDecompressor());
//				}
//				parent.upgraded.onComplete();
//				return;
//			}
//			parent.upgraded.onError(new IllegalStateException("unknown protocol: " + protocol));
//		}
//
//	}
//
//	static void openStream(Channel ch, ConnectionObserver listener,
//			HttpClientInitializer initializer) {
//		Http2StreamChannelBootstrap http2StreamChannelBootstrap =
//				new Http2StreamChannelBootstrap(ch).handler(new ChannelInitializer() {
//					@Override
//					protected void initChannel(Channel ch) {
//						ch.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(false));
//						ChannelOperations.addReactiveBridge(ch,
//								(conn, l, msg) -> new HttpClientOperations(conn, l,
//										initializer.handler.cookieEncoder,
//										initializer.handler.cookieDecoder),
//								listener);
//						if (log.isDebugEnabled()) {
//							log.debug(format(ch, "Initialized HTTP/2 pipeline {}"), ch.pipeline());
//						}
//						initializer.upgraded.onComplete();
//					}
//				});

	static final class H2Initializer
			implements BiConsumer<ConnectionObserver, Channel>  {

		final HttpClientHandler handler;

		final Http2StreamFrameToHttpObjectCodec httpToH2Codec;

		H2Initializer(HttpClientHandler handler) {
			this.handler = handler;
			this.httpToH2Codec = new Http2StreamFrameToHttpObjectCodec(false, true);

		}

		@Override
		public void accept(ConnectionObserver listener, Channel channel) {
			ChannelPipeline p = channel.pipeline();

//			Http2MultiplexCodecBuilder http2MultiplexCodecBuilder =
//					Http2MultiplexCodecBuilder.forClient(new Http2StreamInitializer(listener, handler.cookieEncoder, handler.cookieDecoder))
//					                          .validateHeaders(handler.decoder.validateHeaders())
//					                          .initialSettings(Http2Settings.defaultSettings());
//
			final Http2Connection connection = new DefaultHttp2Connection(false);

			HttpToHttp2ConnectionHandlerBuilder http2ConnectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
					.connection(connection);

			HttpToH2OutboundHandlerAdapter httpToH2FrameAdapter = new HttpToH2OutboundHandlerAdapter(connection, handler.decoder.validateHeaders());

			if (handler.compress) {
				http2ConnectionHandler.frameListener(new DelegatingDecompressorFrameListener(connection, httpToH2FrameAdapter));
			}
			else {
				http2ConnectionHandler.frameListener(httpToH2FrameAdapter);
			}


			if (p.get(NettyPipeline.LoggingHandler) != null) {
				http2ConnectionHandler.frameLogger(new Http2FrameLogger(LogLevel.DEBUG, "reactor.netty.http.client.h2.secured"));
			}

			p.addLast(NettyPipeline.HttpCodec, http2ConnectionHandler.build());
			p.addLast(NettyPipeline.HttpCodec+"-dataOutbound", httpToH2FrameAdapter);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			H2Initializer that = (H2Initializer) o;
			return handler.compress == that.handler.compress;
		}

		@Override
		public int hashCode() {
			return handler.compress ? 1 : 0;
		}

//		final static class H2ToHttpFrameListener extends Http2EventAdapter {
//
//			final Http2Connection connection;
//
//			H2ToHttpFrameListener(Http2Connection connection) {
//				this.connection = connection;
//			}
//
//			@Override
//			public void onStreamRemoved(Http2Stream stream) {
////				stream.getProperty()
//			}
//
//			FullHttpMessage processHeadersBegin(ChannelHandlerContext ctx, Http2Stream stream, Http2Headers headers,
//					boolean endOfStream, boolean allowAppend, boolean appendToTrailer) throws Http2Exception {
//				FullHttpMessage msg = getMessage(stream);
//				boolean release = true;
//				if (msg == null) {
//					msg = HttpConversionUtil.toHttpResponse(stream.id(), headers, ctx.alloc(), true);
//				}
//				else if (allowAppend) {
//					release = false;
//					HttpConversionUtil.addHttp2ToHttpHeaders(stream.id(), headers, msg, appendToTrailer);
//				}
//				else {
//					release = false;
//					msg = null;
//				}
//
//				if (sendDetector.mustSendImmediately(msg)) {
//					// Copy the message (if necessary) before sending. The content is not expected to be copied (or used) in
//					// this operation but just in case it is used do the copy before sending and the resource may be released
//					final FullHttpMessage copy = endOfStream ? null : sendDetector.copyIfNeeded(msg);
//					fireChannelRead(ctx, msg, release, stream);
//					return copy;
//				}
//
//				return msg;
//			}
//
//			/**
//			 * After HTTP/2 headers have been processed by {@link #processHeadersBegin} this method either
//			 * sends the result up the pipeline or retains the message for future processing.
//			 *
//			 * @param ctx The context for which this message has been received
//			 * @param stream The stream the {@code objAccumulator} corresponds to
//			 * @param msg The object which represents all headers/data for corresponding to {@code stream}
//			 * @param endOfStream {@code true} if this is the last event for the stream
//			 */
//			private void processHeadersEnd(ChannelHandlerContext ctx, Http2Stream stream, FullHttpMessage msg,
//					boolean endOfStream) {
//				if (endOfStream) {
//					// Release if the msg from the map is different from the object being forwarded up the pipeline.
//					fireChannelRead(ctx, msg, getMessage(stream) != msg, stream);
//				} else {
//					putMessage(stream, msg);
//				}
//			}
//
//			@Override
//			public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
//					throws Http2Exception {
//				Http2Stream stream = connection.stream(streamId);
//				FullHttpMessage msg = getMessage(stream);
//				if (msg == null) {
//					throw connectionError(PROTOCOL_ERROR, "Data Frame received for unknown stream id %d", streamId);
//				}
//
//				ByteBuf content = msg.content();
//				final int dataReadableBytes = data.readableBytes();
//				if (content.readableBytes() > maxContentLength - dataReadableBytes) {
//					throw connectionError(INTERNAL_ERROR,
//							"Content length exceeded max of %d for stream id %d", maxContentLength, streamId);
//				}
//
//				content.writeBytes(data, data.readerIndex(), dataReadableBytes);
//
//				if (endOfStream) {
//					fireChannelRead(ctx, msg, false, stream);
//				}
//
//				// All bytes have been processed.
//				return dataReadableBytes + padding;
//			}
//
//			@Override
//			public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
//					boolean endOfStream) throws Http2Exception {
//				Http2Stream stream = connection.stream(streamId);
//				FullHttpMessage msg = processHeadersBegin(ctx, stream, headers, endOfStream, true, true);
//				if (msg != null) {
//					processHeadersEnd(ctx, stream, msg, endOfStream);
//				}
//			}
//
//			@Override
//			public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
//					short weight, boolean exclusive, int padding, boolean endOfStream) throws Http2Exception {
//				Http2Stream stream = connection.stream(streamId);
//				FullHttpMessage msg = processHeadersBegin(ctx, stream, headers, endOfStream, true, true);
//				if (msg != null) {
//					// Add headers for dependency and weight.
//					// See https://github.com/netty/netty/issues/5866
//					if (streamDependency != Http2CodecUtil.CONNECTION_STREAM_ID) {
//						msg.headers().setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text(),
//								streamDependency);
//					}
//					msg.headers().setShort(HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text(), weight);
//
//					processHeadersEnd(ctx, stream, msg, endOfStream);
//				}
//			}
//
//			@Override
//			public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws Http2Exception {
//				Http2Stream stream = connection.stream(streamId);
//				FullHttpMessage msg = getMessage(stream);
//				if (msg != null) {
//					onRstStreamRead(stream, msg);
//				}
//				ctx.fireExceptionCaught(Http2Exception.streamError(streamId, Http2Error.valueOf(errorCode),
//						"HTTP/2 to HTTP layer caught stream reset"));
//			}
//
//			@Override
//			public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId,
//					Http2Headers headers, int padding) throws Http2Exception {
//				// A push promise should not be allowed to add headers to an existing stream
//				Http2Stream promisedStream = connection.stream(promisedStreamId);
//				if (headers.status() == null) {
//					// A PUSH_PROMISE frame has no Http response status.
//					// https://tools.ietf.org/html/rfc7540#section-8.2.1
//					// Server push is semantically equivalent to a server responding to a
//					// request; however, in this case, that request is also sent by the
//					// server, as a PUSH_PROMISE frame.
//					headers.status(OK.codeAsText());
//				}
//				FullHttpMessage msg = processHeadersBegin(ctx, promisedStream, headers, false, false, false);
//				if (msg == null) {
//					throw connectionError(PROTOCOL_ERROR, "Push Promise Frame received for pre-existing stream id %d",
//							promisedStreamId);
//				}
//
//				msg.headers().setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_PROMISE_ID.text(), streamId);
//				msg.headers().setShort(HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text(),
//						Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT);
//
//				processHeadersEnd(ctx, promisedStream, msg, false);
//			}
//
//			@Override
//			public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
//				if (propagateSettings) {
//					// Provide an interface for non-listeners to capture settings
//					ctx.fireChannelRead(settings);
//				}
//			}
//
//		}
	}
//		http2StreamChannelBootstrap.open()
//		                           .addListener(initializer);
//	}

	static void addStreamHandlers(Channel ch, ConnectionObserver listener, ClientCookieEncoder encoder, ClientCookieDecoder decoder) {
		ch.pipeline()
//		  .addLast(new Http2StreamBridgeHandler(listener, encoder, decoder))
		  .addLast(new Http2StreamFrameToHttpObjectCodec(false));

		ChannelOperations.addReactiveBridge(ch, ChannelOperations.OnSetup.empty(), listener);

		if (log.isDebugEnabled()) {
			log.debug(format(ch, "Initialized HTTP/2 pipeline {}"), ch.pipeline());
		}
	}

	@ChannelHandler.Sharable
	static final class Http2StreamInitializer extends ChannelInitializer<Channel> {

		final ConnectionObserver  listener;
		final ClientCookieEncoder cookieEncoder;
		final ClientCookieDecoder cookieDecoder;

		Http2StreamInitializer(ConnectionObserver listener,
				ClientCookieEncoder cookieEncoder,
				ClientCookieDecoder cookieDecoder) {
			this.listener = listener;
			this.cookieEncoder = cookieEncoder;
			this.cookieDecoder = cookieDecoder;
		}


			@Override
		protected void initChannel(Channel ch) {
			addStreamHandlers(ch, listener, cookieEncoder, cookieDecoder);
		}
	}

	static final HttpClientConnect INSTANCE = new HttpClientConnect();
	static final AsciiString       ALL      = new AsciiString("*/*");
	static final Logger            log      = Loggers.getLogger(HttpClientConnect.class);


	static final BiFunction<String, Integer, InetSocketAddress> URI_ADDRESS_MAPPER =
			InetSocketAddressUtil::createUnresolved;
}
