package org.springframework.experimental.jdkclient;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpRequest;
import jdk.incubator.http.HttpResponse;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
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

	private final HttpClient httpClient;

	private final ReactiveAdapter flowAdapter = ReactiveAdapterRegistry.getSharedInstance().getAdapter(Flow.Publisher.class);

	private final HttpMethod httpMethod;

	private final URI uri;

	private final Builder builder;

	private Mono<ClientHttpResponse> response;


	public JdkClientHttpRequest(HttpClient httpClient, HttpMethod httpMethod, URI uri) {
		Assert.notNull(httpClient, "HttpClient should not be null");
		Assert.notNull(httpMethod, "HttpMethod should not be null");
		Assert.notNull(uri, "URI should not be null");
		this.httpClient = httpClient;
		this.uri = uri;
		this.httpMethod = httpMethod;
		this.builder = newBuilder(uri);
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
			this.response = Mono.fromFuture(this.httpClient.sendAsync(request, new PublishingBodyHandler())).map(JdkClientHttpResponse::new);
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
			this.response = Mono.fromFuture(httpClient.sendAsync(request, new PublishingBodyHandler())).map(JdkClientHttpResponse::new);
			return Mono.empty();
		});
	}

	public Mono<ClientHttpResponse> getResponse() {
		return this.response.log();
	}

	static class PublishingBodyHandler implements HttpResponse.BodyHandler<Publisher<List<ByteBuffer>>> {
		@Override
		public HttpResponse.BodySubscriber<Publisher<List<ByteBuffer>>> apply(int statusCode, jdk.incubator.http.HttpHeaders responseHeaders) {
			return new EmitterProcessorBodySubscriber();
		}
	}

	// Could be included in JDK 11, see https://bugs.openjdk.java.net/browse/JDK-8201186
	static class EmitterProcessorBodySubscriber implements HttpResponse.BodySubscriber<Publisher<List<ByteBuffer>>> {

		private final EmitterProcessor<List<ByteBuffer>> processor = EmitterProcessor.create();

		@Override
		public void onSubscribe(Flow.Subscription subscription) {
			this.processor.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					subscription.request(n);
				}

				@Override
				public void cancel() {
					subscription.cancel();
				}
			});
		}

		@Override
		public void onNext(List<ByteBuffer> item) {
			this.processor.onNext(item);
		}

		@Override
		public void onError(Throwable throwable) {
			this.processor.onError(throwable);
		}

		@Override
		public void onComplete() {
			this.processor.onComplete();
		}

		@Override
		public CompletionStage<Publisher<List<ByteBuffer>>> getBody() {
			return CompletableFuture.completedStage(this.processor);
		}
	}

}
