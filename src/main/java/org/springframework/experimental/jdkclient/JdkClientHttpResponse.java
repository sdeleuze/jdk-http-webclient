package org.springframework.experimental.jdkclient;

import java.nio.ByteBuffer;
import java.util.List;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.concurrent.Flow;

import reactor.core.publisher.Flux;

import org.springframework.core.ReactiveAdapter;
import org.springframework.core.ReactiveAdapterRegistry;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.client.reactive.ClientHttpResponse;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * {@link ClientHttpResponse} implementation for the JDK 10 HTTP client.
 *
 * @author Sebastien Deleuze
 * @see HttpClient
 */
public class JdkClientHttpResponse implements ClientHttpResponse {

	private final HttpResponse<Flow.Publisher<List<ByteBuffer>>> httpResponse;

	private final ReactiveAdapter flowAdapter = ReactiveAdapterRegistry.getSharedInstance().getAdapter(Flow.Publisher.class);

	private final Flux<DataBuffer> content;

	private final DataBufferFactory factory = new DefaultDataBufferFactory();

	public JdkClientHttpResponse(HttpResponse<Flow.Publisher<List<ByteBuffer>>> httpResponse) {
		Assert.notNull(httpResponse, "HttpResponse should not be null");
		this.httpResponse = httpResponse;
		this.content = Flux.<List<ByteBuffer>>from(flowAdapter.toPublisher(httpResponse.body())).flatMap(Flux::fromIterable).map(this.factory::wrap);
	}

	@Override
	public HttpStatus getStatusCode() {
		return HttpStatus.valueOf(this.httpResponse.statusCode());
	}

	@Override
	public int getRawStatusCode() {
		return this.httpResponse.statusCode();
	}

	@Override
	public MultiValueMap<String, ResponseCookie> getCookies() {
		MultiValueMap<String, ResponseCookie> result = new LinkedMultiValueMap<>();
		getHeaders().get(HttpHeaders.SET_COOKIE).forEach(header -> {
			java.net.HttpCookie.parse(header).forEach(cookie -> result.add(cookie.getName(), ResponseCookie.from(cookie.getName(), cookie.getValue())
					.domain(cookie.getDomain())
					.path(cookie.getPath())
					.maxAge(cookie.getMaxAge())
					.secure(cookie.getSecure())
					.httpOnly(cookie.isHttpOnly())
					.build()));

		});
		return CollectionUtils.unmodifiableMultiValueMap(result);
	}

	@Override
	public Flux<DataBuffer> getBody() {
		return this.content;
	}

	@Override
	public HttpHeaders getHeaders() {
		HttpHeaders headers = new HttpHeaders();
		this.httpResponse.headers().map().entrySet()
				.forEach(e -> e.getValue().forEach(v -> headers.add(e.getKey(), v)));
		return headers;
	}

	@Override
	public String toString() {
		return "JdkClientHttpResponse{" +
				"request=[" + this.httpResponse.request().method() + " " + this.httpResponse.request().uri() + "]," +
				"status=" + getStatusCode() + '}';
	}
}
