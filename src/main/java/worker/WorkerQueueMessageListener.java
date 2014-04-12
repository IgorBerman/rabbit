package worker;

import java.util.Map;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;


public class WorkerQueueMessageListener implements OutputCollector {
    private static final Logger logger = Logger.getLogger(WorkerQueueMessageListener.class);
    private PythonWorker pythonWorker;
    private JavaWorker javaWorker;

    public WorkerQueueMessageListener(Map<String, Object> workerConf, WorkerConf context) {
        pythonWorker = new PythonWorker("python", "tester_worker.py");
        pythonWorker.prepare(workerConf, context, this);
        javaWorker = new JavaWorker();
        javaWorker.prepare(workerConf, context, this);
    }

    @PreDestroy
    public void destroy() {
        pythonWorker.cleanup();
    }

    public void handleMessage(WorkMessage input) {
        logger.info(Thread.currentThread() + " got " + input);
        if (input.isPython()) {
            pythonWorker.execute(input);
        } else {
            javaWorker.execute(input);
        }
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
