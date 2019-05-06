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

package reactor.netty.channel;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;

final class MonoSendScalar<I, O> extends MonoSend<I, O> {

	//Use in 0.9+
	static MonoSendScalar<ByteBuf, ByteBuf> byteBufSource(ByteBuf source, Channel channel) {
		return new MonoSendScalar<>(source, channel, FUNCTION_BB_IDENTITY, CONSUMER_BB_NOCHECK_CLEANUP, SIZE_OF_BB, emptyPostWrite());
	}

	static <I> MonoSendScalar<I, ?> objectSource(I source, Channel channel, Function<? super I, ?> transformer, Consumer<? super I> postWrite) {
		return new MonoSendScalar<>(source, channel, transformer, CONSUMER_NOCHECK_CLEANUP, SIZE_OF, postWrite);
	}

	final I                   source;
	final Consumer<? super I> postWrite;

	MonoSendScalar(I source,
			Channel channel,
			Function<? super I, ? extends O> transformer,
			Consumer<? super I> sourceCleanup,
			ToIntFunction<O> sizeOf,
			Consumer<? super I> postWrite) {
		super(channel, transformer, sourceCleanup, sizeOf);
		this.source = Objects.requireNonNull(source, "source data cannot be null");
		this.postWrite = Objects.requireNonNull(postWrite, "postWrite callback cannot be null");
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> destination) {

		SendScalarInner<I, O> sender = new SendScalarInner<>(this, destination, source, postWrite);
		destination.onSubscribe(sender);
		if (ctx.channel().eventLoop().inEventLoop()){
			sender.run();
		}
		else {
			ctx.channel().eventLoop().execute(sender);
		}

	}

	static final class SendScalarInner<I, O> implements Subscription, Runnable, Scannable, ChannelPromise {

		final I                            source;
		final MonoSend<I, O>               parent;
		final CoreSubscriber<? super Void> actual;
		final Consumer<? super I>          writeCleanup;

		boolean running;
		boolean done;
		Throwable error;

		volatile int terminated;

		static final int STATE_PENDING                                     = 0;
		static final int STATE_RUN                                         = 1;
		static final int STATE_CANCELLED                                   = 2;
		static final AtomicIntegerFieldUpdater<SendScalarInner> TERMINATED = AtomicIntegerFieldUpdater.newUpdater(SendScalarInner.class, "terminated");

		SendScalarInner(MonoSend<I, O> parent,
				CoreSubscriber<? super Void> actual,
				I source,
				Consumer<? super I> writeCleanup) {
			this.parent = parent;
			this.source = source;
			this.actual = actual;
			this.writeCleanup = writeCleanup;
		}

		@Override
		public void cancel() {
			if (done) {
				return;
			}
			if (TERMINATED.compareAndSet(this, STATE_PENDING, STATE_CANCELLED)) {
				cleanup();
			}
		}

		@Override
		public void request(long n) {
			//ignore since downstream has no demand
		}

		@Override
		public void run() {
			if (!TERMINATED.compareAndSet(this, STATE_PENDING, STATE_RUN)) {
				return;
			}
			try {
				running = true;
				parent.ctx.writeAndFlush(parent.transformer.apply(source), this);
				running = false;
			}
			catch (Throwable err) {
				Exceptions.throwIfJvmFatal(err);
				cleanup();
				return;
			}

			if (done) {
				writeCleanup.accept(source);
				if (error == null) {
					actual.onComplete();
				}
				else {
					actual.onError(error);
				}
			}
		}

		void cleanup() {
			parent.sourceCleanup.accept(source);
			Operators.onDiscard(source, actual.currentContext());
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ACTUAL) return actual;
			if (key == Attr.CANCELLED) return terminated == 2;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return done ? 0 : 1;
			if (key == Attr.ERROR) return error;
			return null;
		}

		@Override
		public Channel channel() {
			return parent.ctx.channel();
		}

		@Override
		public ChannelPromise setSuccess(Void result) {
			trySuccess(null);
			return this;
		}

		@Override
		public ChannelPromise setSuccess() {
			trySuccess(null);
			return this;
		}

		@Override
		public boolean trySuccess() {
			trySuccess(null);
			return true;
		}

		@Override
		public ChannelPromise setFailure(Throwable cause) {
			if (tryFailure(cause)) {
				return this;
			}
			Operators.onErrorDropped(cause, actual.currentContext());
			return this;
		}

		@Override
		public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
			return this;
		}

		@Override
		public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
			return this;
		}

		@Override
		public ChannelPromise sync() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise syncUninterruptibly() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise await()  {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise awaitUninterruptibly() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ChannelPromise unvoid() {
			return this;
		}

		@Override
		public boolean isVoid() {
			return true;
		}

		@Override
		public boolean trySuccess(Void result) {
			if (done) {
				return false;
			}
			done = true;
			if (!running) {
				writeCleanup.accept(source);
				running = true;
				actual.onComplete();
			}
			return true;
		}

		@Override
		public boolean tryFailure(Throwable cause) {
			if (done) {
				return false;
			}
			done = true;
			error = cause;
			if (!running) {
				writeCleanup.accept(source);
				running = true;
				actual.onError(cause);
			}
			return true;
		}

		@Override
		public boolean setUncancellable() {
			return true;
		}

		@Override
		public boolean isSuccess() {
			return done && error == null;
		}

		@Override
		public boolean isCancellable() {
			return false;
		}

		@Override
		public Throwable cause() {
			return error;
		}

		@Override
		public boolean await(long timeout, TimeUnit unit) {
			return false;
		}

		@Override
		public boolean await(long timeoutMillis) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean awaitUninterruptibly(long timeoutMillis) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Void getNow() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return done;
		}

		@Override
		public Void get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Void get(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("unchecked")
	static <I> Consumer<? super I> emptyPostWrite(){
		return (Consumer<? super I>)EMPTY_WRITE_CLEANUP;
	}

	static Consumer EMPTY_WRITE_CLEANUP = d -> {};

	static final Logger log = Loggers.getLogger(MonoSendScalar.class);
}
