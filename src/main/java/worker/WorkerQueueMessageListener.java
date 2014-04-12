package worker;

import java.util.Map;

import org.apache.log4j.Logger;


public class WorkerQueueMessageListener implements OutputCollector {
    private static final Logger logger = Logger.getLogger(WorkerQueueMessageListener.class);
    private Worker worker;

    public WorkerQueueMessageListener(Map<String, Object> workerConf, WorkerConf context) {
        worker = new Worker("python", "tester_worker.py");
        worker.prepare(workerConf, context, this);

    }

    public void handleMessage(WorkMessage input) {
        logger.info(Thread.currentThread() + " got " + input);
        worker.execute(input);
    }

    @Override
    public void ack(WorkMessage acked) {
        logger.info("Acked " + acked);

    }

    @Override
    public void fail(WorkMessage failed) {
        logger.error("Failed " + failed);

    }

    @Override
    public void reportError(Exception exception) {
        logger.error("Error " + exception);
    }
}
