package com.sarva.rabbitmqpublisher;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class RabbitmqPublisherApplication {

	static final String TOPIC_EXCHANGE_NAME = "test-exchange";

	private static final String QUEUE_NAME = "test-queue";

	@Bean
	Queue queue() {
		return new Queue(QUEUE_NAME, false);
	}

	@Bean
	TopicExchange exchange() {
		return new TopicExchange(TOPIC_EXCHANGE_NAME);
	}

	@Bean
	Binding binding(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.objcreated");
	}

	@Bean
	Binding binding1(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj1updated");
	}

	@Bean
	Binding binding2(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj2updated");
	}

	@Bean
	Binding binding3(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj3updated");
	}

	@Bean
	Binding binding4(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj4updated");
	}

	@Bean
	Binding binding5(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj5updated");
	}

	@Bean
	Binding binding6(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj6updated");
	}

	@Bean
	Binding binding7(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj7updated");
	}

	@Bean
	Binding binding8(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj8updated");
	}

	@Bean
	Binding binding9(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj9updated");
	}

	@Bean
	Binding binding10(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with("test.key.obj10updated");
	}

	@Bean
	SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
			MessageListenerAdapter listenerAdapter) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames(QUEUE_NAME);
		container.setMessageListener(listenerAdapter);
		return container;
	}

	@Bean
	MessageListenerAdapter listenerAdapter(Receiver receiver) {
		return new MessageListenerAdapter(receiver, "receiveMessage");
	}

	public static void main(String[] args) {
		SpringApplication.run(RabbitmqPublisherApplication.class, args);
	}

}

@RestController()
class PublishController {

	private final RabbitTemplate rabbitTemplate;

	public PublishController(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}

	@GetMapping
	public String sayHello() {
		return "hi!";
	}

	@GetMapping("/publish/{count}")
	public String publishMessage(@PathVariable int count) {
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			rabbitTemplate.convertAndSend(RabbitmqPublisherApplication.TOPIC_EXCHANGE_NAME, "test.key.objcreated",
					"test message " + i);
		}
		long timeTaken = System.currentTimeMillis() - startTime;
		return "total time taken to publish " + count + " message: " + timeTaken + " millisecs";
	}

}
