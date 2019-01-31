package org.springframework.experimental.jdkclient;

import static java.net.http.HttpRequest.BodyPublishers;
import static java.net.http.HttpRequest.Builder;
import static java.net.http.HttpRequest.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;
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
			HttpRequest request = this.builder.method(this.httpMethod.name(), BodyPublishers.fromPublisher(publisher)).build();
			this.response = Mono.fromFuture(this.httpClient.sendAsync(request, BodyHandlers.ofPublisher())).map(JdkClientHttpResponse::new);
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
			HttpRequest request = this.builder.method(this.httpMethod.name(), BodyPublishers.noBody()).build();
			this.response = Mono.fromFuture(httpClient.sendAsync(request, BodyHandlers.ofPublisher())).map(JdkClientHttpResponse::new);
			return Mono.empty();
		});
	}

	public Mono<ClientHttpResponse> getResponse() {
		return this.response.log();
	}

}
