/*
 * Copyright 2002-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.test.experimental.jdkclient;

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoProcessor;
import reactor.test.StepVerifier;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.experimental.jdkclient.JdkClientHttpConnector;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.test.experimental.jdkclient.bootstrap.HttpServer;
import org.springframework.test.experimental.jdkclient.bootstrap.ReactorHttpServer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;
import static org.springframework.http.MediaType.TEXT_EVENT_STREAM;
import static org.springframework.web.reactive.function.BodyExtractors.toFlux;

@RunWith(Parameterized.class)
public class SseIntegrationTests {

	private AnnotationConfigApplicationContext wac;

	private WebClient webClient;

	@Parameterized.Parameter(0)
	public ClientHttpConnector connector;

	@Parameterized.Parameters(name = "client [{0}]")
	public static Object[][] arguments() {
		return new Object[][] {
				{new JdkClientHttpConnector()},
				{new ReactorClientHttpConnector()}
		};
	}

	private HttpServer server = new ReactorHttpServer();

	private int port;

	@Before
	public void setup() throws Exception {
		this.server.setHandler(createHttpHandler());
		this.server.afterPropertiesSet();
		this.server.start();

		// Set dynamically chosen port
		this.port = this.server.getPort();

		this.webClient = WebClient
				.builder()
				.clientConnector(this.connector)
				.baseUrl("http://localhost:" + this.port + "/sse")
				.build();
	}

	@After
	public void tearDown() throws Exception {
		this.server.stop();
		this.port = 0;
	}


	protected HttpHandler createHttpHandler() {
		this.wac = new AnnotationConfigApplicationContext();
		this.wac.register(TestConfiguration.class);
		this.wac.refresh();

		return WebHttpHandlerBuilder.webHandler(new DispatcherHandler(this.wac)).build();
	}

	@Test
	public void sseAsString() {
		Flux<String> result = this.webClient.get()
				.uri("/string")
				.accept(TEXT_EVENT_STREAM)
				.exchange()
				.flatMapMany(response -> response.bodyToFlux(String.class));

		StepVerifier.create(result)
				.expectNext("foo 0")
				.expectNext("foo 1")
				.thenCancel()
				.verify(Duration.ofSeconds(5L));
	}

	@Test
	public void sseAsPerson() {
		Flux<Person> result = this.webClient.get()
				.uri("/person")
				.accept(TEXT_EVENT_STREAM)
				.exchange()
				.flatMapMany(response -> response.bodyToFlux(Person.class));

		StepVerifier.create(result)
				.expectNext(new Person("foo 0"))
				.expectNext(new Person("foo 1"))
				.thenCancel()
				.verify(Duration.ofSeconds(5L));
	}

	@Test
	public void sseAsEvent() {
		Flux<ServerSentEvent<String>> result = this.webClient.get()
				.uri("/event")
				.accept(TEXT_EVENT_STREAM)
				.exchange()
				.flatMapMany(response -> response.body(
						toFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})));

		StepVerifier.create(result)
				.consumeNextWith( event -> {
					assertEquals("0", event.id());
					assertEquals("foo", event.data());
					assertEquals("bar", event.comment());
					assertNull(event.event());
					assertNull(event.retry());
				})
				.consumeNextWith( event -> {
					assertEquals("1", event.id());
					assertEquals("foo", event.data());
					assertEquals("bar", event.comment());
					assertNull(event.event());
					assertNull(event.retry());
				})
				.thenCancel()
				.verify(Duration.ofSeconds(5L));
	}

	@Test
	public void sseAsEventWithoutAcceptHeader() {
		Flux<ServerSentEvent<String>> result = this.webClient.get()
				.uri("/event")
				.accept(TEXT_EVENT_STREAM)
				.exchange()
				.flatMapMany(response -> response.body(
						toFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})));

		StepVerifier.create(result)
				.consumeNextWith( event -> {
					assertEquals("0", event.id());
					assertEquals("foo", event.data());
					assertEquals("bar", event.comment());
					assertNull(event.event());
					assertNull(event.retry());
				})
				.consumeNextWith( event -> {
					assertEquals("1", event.id());
					assertEquals("foo", event.data());
					assertEquals("bar", event.comment());
					assertNull(event.event());
					assertNull(event.retry());
				})
				.thenCancel()
				.verify(Duration.ofSeconds(5L));
	}

	@Test // SPR-16494
	@Ignore // https://github.com/reactor/reactor-netty/issues/283
	public void serverDetectsClientDisconnect() {

		assumeTrue(this.server instanceof ReactorHttpServer);

		Flux<String> result = this.webClient.get()
				.uri("/infinite")
				.accept(TEXT_EVENT_STREAM)
				.exchange()
				.flatMapMany(response -> response.bodyToFlux(String.class));

		StepVerifier.create(result)
				.expectNext("foo 0")
				.expectNext("foo 1")
				.thenCancel()
				.verify(Duration.ofSeconds(5L));

		SseController controller = this.wac.getBean(SseController.class);
		controller.cancellation.block(Duration.ofSeconds(5));
	}


	@RestController
	@SuppressWarnings("unused")
	static class SseController {

		private static final Flux<Long> INTERVAL = interval(Duration.ofMillis(100), 50);

		private MonoProcessor<Void> cancellation = MonoProcessor.create();


		@GetMapping("/sse/string")
		Flux<String> string() {
			return INTERVAL.map(l -> "foo " + l);
		}

		@GetMapping("/sse/person")
		Flux<Person> person() {
			return INTERVAL.map(l -> new Person("foo " + l));
		}

		@GetMapping("/sse/event")
		Flux<ServerSentEvent<String>> sse() {
			return INTERVAL.map(l -> ServerSentEvent.builder("foo")
					.id(Long.toString(l))
					.comment("bar")
					.build());
		}

		@GetMapping("/sse/infinite")
		Flux<String> infinite() {
			return Flux.just(0, 1).map(l -> "foo " + l)
					.mergeWith(Flux.never())
					.doOnCancel(() -> cancellation.onComplete());
		}
	}


	@Configuration
	@EnableWebFlux
	@SuppressWarnings("unused")
	static class TestConfiguration {

		@Bean
		public SseController sseController() {
			return new SseController();
		}
	}


	@SuppressWarnings("unused")
	private static class Person {

		private String name;

		public Person() {
		}

		public Person(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return !(this.name != null ? !this.name.equals(person.name) : person.name != null);
		}

		@Override
		public int hashCode() {
			return this.name != null ? this.name.hashCode() : 0;
		}

		@Override
		public String toString() {
			return "Person{name='" + this.name + '\'' + '}';
		}
	}

	/**
	 * Return an interval stream of N number of ticks and buffer the emissions
	 * to avoid back pressure failures (e.g. on slow CI server).
	 *
	 * <p>Use this method as follows:
	 * <ul>
	 * <li>Tests that verify N number of items followed by verifyOnComplete()
	 * should set the number of emissions to N.
	 * <li>Tests that verify N number of items followed by thenCancel() should
	 * set the number of buffered to an arbitrary number greater than N.
	 * </ul>
	 */
	public static Flux<Long> interval(Duration period, int count) {
		return Flux.interval(period).take(count).onBackpressureBuffer(count);
	}

}