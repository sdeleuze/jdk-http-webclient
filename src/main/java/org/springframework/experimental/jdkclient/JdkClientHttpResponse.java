package org.springframework.experimental.jdkclient;

import jdk.incubator.http.HttpClient;
import jdk.incubator.http.HttpResponse;
import reactor.core.publisher.Flux;

import org.springframework.core.io.buffer.DataBuffer;
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

	private final HttpResponse<?> httpResponse;

	private final Flux<DataBuffer> content;

	public JdkClientHttpResponse(HttpResponse<?> httpResponse, Flux<DataBuffer> content) {
		Assert.notNull(httpResponse, "HttpResponse should not be null");
		Assert.notNull(content, "Content should not be null");
		this.httpResponse = httpResponse;
		this.content = content;
	}

	@Override
	public HttpStatus getStatusCode() {
		return HttpStatus.valueOf(this.httpResponse.statusCode());
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
