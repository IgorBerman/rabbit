package worker;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
public class SpringConfiguration {

    private static final String PID_DIR = "c:/tmp/pid";
    private static final int CONCURRENT_CONSUMERS = 3;
    private static final int PREFETCH_COUNT = 3;
    private static final String QUEUE_BROKER_HOSTNAME = "localhost";
    public static final String WORKER_QUEUE_NAME = "workerQueue";

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(QUEUE_BROKER_HOSTNAME);
        return connectionFactory;
    }

    @Bean
    public SimpleMessageListenerContainer messageListenerContainer() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory());
        container.setQueueNames(WORKER_QUEUE_NAME);
        container.setPrefetchCount(PREFETCH_COUNT);
        container.setConcurrentConsumers(CONCURRENT_CONSUMERS);// 3 consumer threads

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(CONCURRENT_CONSUMERS);
        executor.setCorePoolSize(CONCURRENT_CONSUMERS);
        executor.setDaemon(true);
        executor.setThreadNamePrefix(WORKER_QUEUE_NAME + "Consumer");
        executor.initialize();
        container.setTaskExecutor(executor);

        container.setMessageListener(new MessageListenerAdapter(exampleListener(), new JsonMessageConverter()));
        return container;
    }

    @Bean
    public WorkerQueueMessageListener exampleListener() {
        return new WorkerQueueMessageListener(conf(), context());
    }

    @Bean
    public WorkerConf context() {
        File file = new File(PID_DIR);
        file.mkdirs();
        String codeDir = System.getProperty("user.dir") + "/bin";// in eclipse...
        return new WorkerConf(codeDir, PID_DIR);
    }

    @Bean
    public Map<String, Object> conf() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put("a", "b");
        return conf;
    }

    @Bean
    public AmqpAdmin amqpAdmin() {
        return new RabbitAdmin(connectionFactory());
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());
        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        return rabbitTemplate;
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }

    @Bean
    public Queue myQueue() {
        return new Queue(WORKER_QUEUE_NAME);
    }
}
