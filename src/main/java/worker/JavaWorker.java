package worker;

import java.util.Map;

import org.apache.log4j.Logger;


public class JavaWorker {
    private static final Logger logger = Logger.getLogger(JavaWorker.class);
    private Map<String, Object> workerConf;
    private WorkerConf context;
    private OutputCollector outputCollector;

    public void execute(WorkMessage input) {
        logger.info("Executing task in java thread " + input);
    }

    public void prepare(Map<String, Object> workerConf, WorkerConf context, OutputCollector outputCollector) {
        this.workerConf = workerConf;
        this.context = context;
        this.outputCollector = outputCollector;
    }

}
