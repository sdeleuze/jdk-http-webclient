module org.springframework.experimental.jdkclient {
	requires jdk.incubator.httpclient;
	requires spring.core;
	requires spring.web;
	requires org.reactivestreams;
	requires reactor.core;
	exports org.springframework.experimental.jdkclient;
}