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
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import io.netty.channel.Channel;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;

final class MonoSendUsing<I, O> extends MonoSend<I, O> {

	static <I, O> MonoSendUsing<I, O> objectSource(Callable<? extends I> source, Channel channel, Function<? super I, ? extends O> mapper, Consumer<? super I> cleanup) {
		return new MonoSendUsing<>(source, channel, mapper, cleanup, defaultSizeOf());
	}

	final Callable<? extends I> source;

	MonoSendUsing(Callable<? extends I> source,
			Channel channel,
			Function<? super I, ? extends O> transformer,
			Consumer<? super I> sourceCleanup,
			ToIntFunction<O> sizeOf) {
		super(channel, transformer, sourceCleanup, sizeOf);
		this.source = Objects.requireNonNull(source, "source data callable cannot be null");
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> destination) {
		I source;

		try {
			source = Objects.requireNonNull(this.source.call(), "source callable cannot produce null");
		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			Operators.error(destination, err);
			return;
		}

		MonoSendScalar.SendScalarInner<I, O> sender = new MonoSendScalar.SendScalarInner<>(this, destination, source, sourceCleanup);

		destination.onSubscribe(sender);
		if (ctx.channel().eventLoop().inEventLoop()){
			sender.run();
		}
		else {
			ctx.channel().eventLoop().execute(sender);
		}

	}
}
