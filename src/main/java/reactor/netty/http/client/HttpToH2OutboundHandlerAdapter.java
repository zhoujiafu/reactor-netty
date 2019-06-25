package reactor.netty.http.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.HttpConversionUtil;
import reactor.util.annotation.Nullable;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http2.Http2Error.PROTOCOL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.connectionError;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

/**
 * Adapted from {@link io.netty.handler.codec.http2.InboundHttpToHttp2Adapter}
 * @author Stephane Maldini
 */
final class HttpToH2OutboundHandlerAdapter extends ChannelOutboundHandlerAdapter implements Http2Connection.Listener,
                                                                                            Http2FrameListener {

	final Http2Connection.PropertyKey messageKey;
	final Http2Connection             connection;
	final boolean                     validateHttpHeaders;

	HttpToH2OutboundHandlerAdapter(Http2Connection connection, boolean validateHttpHeaders) {
		checkNotNull(connection, "connection");
		this.connection = connection;
		this.validateHttpHeaders = validateHttpHeaders;
		this.messageKey = connection.newKey();
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
		if (msg instanceof ByteBuf) {
			ctx.write(new DefaultHttpContent((ByteBuf) msg), promise);
			return;
		}
		ctx.write(msg, promise);
	}

	@Override
	public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
			throws Http2Exception {
		Http2Stream stream = connection.stream(streamId);
		FullHttpResponse msg = getMessage(stream);
		int readable = data.readableBytes();

		if (msg != null) {
			stream.removeProperty(messageKey);
			if (endOfStream) {
				if (readable != 0) {
					ByteBuf content = msg.content();
					HttpUtil.setContentLength(msg, readable);
					content.writeBytes(data, data.readerIndex(), readable);
				}
				ctx.fireChannelRead(msg);
				return readable + padding;
			}
			ctx.fireChannelRead(new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers()));
		}

		if (endOfStream) {
			if (readable != 0) {
				ctx.fireChannelRead(new DefaultLastHttpContent(data.retain()));
			}
			else {
				ctx.fireChannelRead(LastHttpContent.EMPTY_LAST_CONTENT);
			}
			return readable + padding;
		}

		ctx.fireChannelRead(data.retain());

		return readable + padding;
	}

	@Override
	public void onHeadersRead(ChannelHandlerContext ctx,
			int streamId,
			Http2Headers headers,
			int padding,
			boolean endOfStream) throws Http2Exception {
		Http2Stream stream = connection.stream(streamId);
		FullHttpResponse msg = processHeadersBegin(ctx, stream, headers,true, true);
		if (msg != null) {
			processHeadersEnd(ctx, stream, msg, endOfStream);
		}
	}

	@Override
	public void onHeadersRead(ChannelHandlerContext ctx,
			int streamId,
			Http2Headers headers,
			int streamDependency,
			short weight,
			boolean exclusive,
			int padding,
			boolean endOfStream) throws Http2Exception {
		Http2Stream stream = connection.stream(streamId);
		FullHttpResponse msg = processHeadersBegin(ctx, stream, headers, true, true);
		if (msg != null) {
			// Add headers for dependency and weight.
			// See https://github.com/netty/netty/issues/5866
			if (streamDependency != Http2CodecUtil.CONNECTION_STREAM_ID) {
				msg.headers()
				   .setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text(), streamDependency);
			}
			msg.headers()
			   .setShort(HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text(), weight);

			processHeadersEnd(ctx, stream, msg, endOfStream);
		}
	}

	@Override
	public void onPushPromiseRead(ChannelHandlerContext ctx,
			int streamId,
			int promisedStreamId,
			Http2Headers headers,
			int padding) throws Http2Exception {
		// A push promise should not be allowed to add headers to an existing stream
		Http2Stream promisedStream = connection.stream(promisedStreamId);
		if (headers.status() == null) {
			// A PUSH_PROMISE frame has no Http response status.
			// https://tools.ietf.org/html/rfc7540#section-8.2.1
			// Server push is semantically equivalent to a server responding to a
			// request; however, in this case, that request is also sent by the
			// server, as a PUSH_PROMISE frame.
			headers.status(OK.codeAsText());
		}
		FullHttpResponse msg = processHeadersBegin(ctx, promisedStream, headers, false, false);
		if (msg == null) {
			throw connectionError(PROTOCOL_ERROR,
					"Push Promise Frame received for pre-existing stream id %d",
					promisedStreamId);
		}

		msg.headers()
		   .setInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_PROMISE_ID.text(), streamId);
		msg.headers()
		   .setShort(HttpConversionUtil.ExtensionHeaderNames.STREAM_WEIGHT.text(),
				   Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT);

		processHeadersEnd(ctx, promisedStream, msg, false);
	}

	@Override
	public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws Http2Exception {
		Http2Stream stream = connection.stream(streamId);
		FullHttpResponse msg = getMessage(stream);
		if (msg != null) {
			stream.removeProperty(messageKey);
		}
		ctx.fireExceptionCaught(Http2Exception.streamError(streamId,
				Http2Error.valueOf(errorCode),
				"HTTP/2 to HTTP layer caught stream reset"));
	}

	@Override
	public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
//		ctx.fireChannelRead(settings);
	}

	@Override
	public void onStreamRemoved(Http2Stream stream) {
		stream.removeProperty(messageKey);
	}

	@Override
	public void onStreamAdded(Http2Stream stream) {

	}

	@Override
	public void onStreamActive(Http2Stream stream) {

	}

	@Override
	public void onStreamHalfClosed(Http2Stream stream) {

	}

	@Override
	public void onStreamClosed(Http2Stream stream) {

	}

	@Override
	public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {

	}

	@Override
	public void onGoAwayReceived(int lastStreamId, long errorCode, ByteBuf debugData) {

	}

	@Override
	public void onPriorityRead(ChannelHandlerContext ctx,
			int streamId,
			int streamDependency,
			short weight,
			boolean exclusive) throws Http2Exception {

	}

	@Override
	public void onSettingsAckRead(ChannelHandlerContext ctx) throws Http2Exception {

	}

	@Override
	public void onPingRead(ChannelHandlerContext ctx, long data) throws Http2Exception {

	}

	@Override
	public void onPingAckRead(ChannelHandlerContext ctx, long data) throws Http2Exception {

	}

	@Override
	public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData)
			throws Http2Exception {

	}

	@Override
	public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement)
			throws Http2Exception {

	}

	@Override
	public void onUnknownFrame(ChannelHandlerContext ctx,
			byte frameType,
			int streamId,
			Http2Flags flags,
			ByteBuf payload) throws Http2Exception {

	}

	@Nullable
	FullHttpResponse getMessage(Http2Stream stream) {
		return (FullHttpResponse) stream.getProperty(messageKey);
	}

	void putMessage(Http2Stream stream, FullHttpResponse message) {
		FullHttpResponse previous = stream.setProperty(messageKey, message);
		if (previous != message && previous != null) {
			previous.release();
		}
	}

	@Nullable
	FullHttpResponse processHeadersBegin(ChannelHandlerContext ctx,
			Http2Stream stream,
			Http2Headers headers,
			boolean allowAppend,
			boolean appendToTrailer) throws Http2Exception {
		FullHttpResponse msg = getMessage(stream);
		if (msg == null) {
			msg = HttpConversionUtil.toFullHttpResponse(stream.id(), headers, ctx.alloc(), validateHttpHeaders);
		}
		else if (allowAppend) {
			HttpConversionUtil.addHttp2ToHttpHeaders(stream.id(), headers, msg, appendToTrailer);
		}
		else {
			msg = null;
		}

		if (msg != null && msg.status().codeClass() == HttpStatusClass.INFORMATIONAL) {
			stream.removeProperty(messageKey);
			ctx.fireChannelRead(msg);
			return null;
		}

		return msg;
	}

	void processHeadersEnd(ChannelHandlerContext ctx,
			Http2Stream stream,
			FullHttpResponse msg,
			boolean endOfStream) {
		if (endOfStream) {
			// Release if the msg from the map is different from the object being forwarded up the pipeline.
			stream.removeProperty(messageKey);
			ctx.fireChannelRead(msg);
		}
		else {
			putMessage(stream, msg);
		}
	}
}
