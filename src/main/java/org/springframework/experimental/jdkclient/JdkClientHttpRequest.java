package org.springframework.experimental.jdkclient;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Flow;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

import org.springframework.core.ReactiveAdapter;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.reactive.AbstractClientHttpRequest;
import org.springframework.http.client.reactive.ClientHttpRequest;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.util.Assert;

import static jdk.incubator.http.HttpRequest.BodyPublisher;
import static jdk.incubator.http.HttpRequest.Builder;
import static jdk.incubator.http.HttpRequest.newBuilder;

/**
 * {@link ClientHttpRequest} implementation for the JDK 10 HTTP client.
 *
 * @author Sebastien Deleuze
 * @see HttpClient
 */
public class JdkClientHttpRequest extends AbstractClientHttpRequest {

	private final HttpClient httpClient = HttpClient.newHttpClient();

	private final ReactiveAdapter flowAdapter = ReactiveAdapterRegistry.getSharedInstance().getAdapter(Flow.Publisher.class);

	private final HttpMethod httpMethod;

	private final URI uri;

	private final Builder builder;

	private Mono<ClientHttpResponse> response;


	public JdkClientHttpRequest(HttpMethod httpMethod, URI uri) {
		Assert.notNull(httpMethod, "HttpMethod should not be null");
		Assert.notNull(uri, "URI should not be null");
		this.uri = uri;
		this.httpMethod = httpMethod;
		// TODO HTTP/2 support
		this.builder = newBuilder(uri).version(HttpClient.Version.HTTP_1_1);
	}

	@Override
	protected void applyHeaders() {
		getHeaders().entrySet().forEach(e -> e.getValue().forEach(v -> this.builder.header(e.getKey(), v)));
		if (!getHeaders().containsKey(HttpHeaders.ACCEPT)) {
			this.builder.header(HttpHeaders.ACCEPT, "*/*");
		}
	}

	@Override
	protected void applyCookies() {
		getCookies().values().stream().flatMap(Collection::stream)
				.forEach(cookie -> this.builder.header(HttpHeaders.COOKIE, cookie.toString()));
	}

	@Override
	public HttpMethod getMethod() {
		return this.httpMethod;
	}

	@Override
	public URI getURI() {
		return this.uri;
	}

	@Override
	public DataBufferFactory bufferFactory() {
		return new DefaultDataBufferFactory();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
		return doCommit(() -> {
			Flow.Publisher<ByteBuffer> publisher = (Flow.Publisher<ByteBuffer>) this.flowAdapter.fromPublisher(Flux.from(body).map(DataBuffer::asByteBuffer));
			HttpRequest request = this.builder.method(this.httpMethod.name(), BodyPublisher.fromPublisher(publisher)).build();
			FluxProcessor<List<ByteBuffer>, List<ByteBuffer>> processor = EmitterProcessor.create();
			HttpResponse.BodyHandler<Void> bodyHandler = HttpResponse.BodyHandler.fromSubscriber(new SubscriberToRS<>(processor));
			Flux<DataBuffer> content = processor.flatMap(Flux::fromIterable).map(buffer -> this.bufferFactory().wrap(buffer)).log();
			this.response = Mono.fromFuture(this.httpClient.sendAsync(request, bodyHandler)).map(r -> new JdkClientHttpResponse(r, content));
			return Mono.empty();
		});
	}

	@Override
	public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
		return writeWith(Flux.from(body).flatMap(buffer -> buffer));
	}

	@Override
	public Mono<Void> setComplete() {
		return doCommit(() -> {
			HttpRequest request = this.builder.method(this.httpMethod.name(), BodyPublisher.noBody()).build();
			FluxProcessor<List<ByteBuffer>, List<ByteBuffer>> processor = EmitterProcessor.create();
			HttpResponse.BodyHandler<Void> bodyHandler = HttpResponse.BodyHandler.fromSubscriber(new SubscriberToRS<>(processor));
			Flux<DataBuffer> content = processor.flatMap(Flux::fromIterable).map(buffer -> this.bufferFactory().wrap(buffer));
			this.response = Mono.fromFuture(httpClient.sendAsync(request, bodyHandler)).map(r -> new JdkClientHttpResponse(r, content));
			return Mono.empty();
		});
	}

	public Mono<ClientHttpResponse> getResponse() {
		return this.response;
	}

	private static class SubscriberToRS<T> implements Flow.Subscriber<T>, Subscription {

		private final Subscriber<? super T> s;

		Flow.Subscription subscription;

		public SubscriberToRS(Subscriber<? super T> s) {
			this.s = s;
		}

		@Override
		public void onSubscribe(final Flow.Subscription subscription) {
			this.subscription = subscription;
			s.onSubscribe(this);
		}

		@Override
		public void onNext(T o) {
			s.onNext(o);
		}

		@Override
		public void onError(Throwable throwable) {
			s.onError(throwable);
		}

		@Override
		public void onComplete() {
			s.onComplete();
		}

		@Override
		public void request(long n) {
			subscription.request(n);
		}

		@Override
		public void cancel() {
			subscription.cancel();
		}
	}

}
