package org.springframework.experimental.jdkclient;

import java.net.URI;
import java.util.function.Function;

import java.net.http.HttpClient;
import reactor.core.publisher.Mono;

import org.springframework.http.HttpMethod;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ClientHttpRequest;
import org.springframework.http.client.reactive.ClientHttpResponse;

/**
 * JDK 11 HTTP client implementation of {@link ClientHttpConnector}.
 *
 * @author Sebastien Deleuze
 */
public class JdkClientHttpConnector implements ClientHttpConnector {

	private final HttpClient httpClient;


	public JdkClientHttpConnector() {
		this.httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
	}

	public JdkClientHttpConnector(HttpClient httpClient) {
		this.httpClient = httpClient;
	}


	@Override
	public Mono<ClientHttpResponse> connect(HttpMethod method, URI uri, Function<? super ClientHttpRequest, Mono<Void>> requestCallback) {

		if (!uri.isAbsolute()) {
			return Mono.error(new IllegalArgumentException("URI is not absolute: " + uri));
		}

		JdkClientHttpRequest request = new JdkClientHttpRequest(this.httpClient, method, uri);
		return requestCallback.apply(request).then(request.getResponse());
	}
}
