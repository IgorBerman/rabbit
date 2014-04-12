package worker;

import java.util.Arrays;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;



public class Main {
    public static void main(final String... args) throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfiguration.class);
        AmqpTemplate template = context.getBean(AmqpTemplate.class);

        template.convertAndSend(SpringConfiguration.WORKER_QUEUE_NAME, new WorkMessage("task", Arrays.asList("arg1")));

        // String foo = (String) template.receiveAndConvert("myqueue");
        Thread.sleep(10000);

        // context.destroy();
        // rabbit mq connector is not-daemon thread, so it won't stop
        // untill we issue destroy on context
    }
}
