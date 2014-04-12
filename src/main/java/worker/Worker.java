package worker;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class Worker {
    private static final String MAX_PENDING = "worker.python.max.pending";
    private static Logger LOG = Logger.getLogger(Worker.class);
    private OutputCollector _collector;
    private Map<String, WorkMessage> _inputs = new ConcurrentHashMap<String, WorkMessage>();

    private String[] _command;
    private ShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private LinkedBlockingQueue<JSONObject> _pendingWrites = new LinkedBlockingQueue<JSONObject>();
    private Random _rand;

    private Thread _readerThread;
    private Thread _writerThread;

    public Worker(String... command) {
        _command = command;
    }

    public void prepare(Map<String, Object> workerConf, WorkerConf context, final OutputCollector collector) {
        Object maxPending = workerConf.get(MAX_PENDING);
        if (maxPending != null) {
            this._pendingWrites = new LinkedBlockingQueue<JSONObject>(((Number) maxPending).intValue());
        }
        _rand = new Random();
        _process = new ShellProcess(_command);
        _collector = collector;

        try {
            // subprocesses must send their pid first thing
            Number subpid = _process.launch(workerConf, context);
            LOG.info("Launched subprocess with pid " + subpid);
        } catch (IOException e) {
            throw new RuntimeException("Error when launching multilang subprocess\n" + _process.getErrorsString(), e);
        }

        // reader
        _readerThread = new Thread(new Runnable() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                while (_running) {
                    try {
                        JSONObject action = _process.readMessage();
                        if (action == null) {
                            // ignore sync
                        }

                        String command = (String) action.get("command");
                        if (command.equals("ack")) {
                            handleAck(action);
                        } else if (command.equals("fail")) {
                            handleFail(action);
                        } else if (command.equals("error")) {
                            handleError(action);
                        } else if (command.equals("log")) {
                            String msg = (String) action.get("msg");
                            LOG.info("Shell msg: " + msg);
                        }
                    } catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });

        _readerThread.start();

        _writerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (_running) {
                    try {
                        Object write = _pendingWrites.poll(1, SECONDS);
                        if (write != null) {
                            _process.writeMessage(write);
                        }
                        // drain the error stream to avoid dead lock because of full error stream buffer
                        _process.drainErrorStream();
                    } catch (InterruptedException e) {} catch (Throwable t) {
                        die(t);
                    }
                }
            }
        });

        _writerThread.start();
    }

    @SuppressWarnings("unchecked")
    public void execute(WorkMessage input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        // just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        try {
            JSONObject obj = new JSONObject();
            obj.put("id", genId);
            obj.put("task", input.getTask());
            obj.put("args", JSONArray.toJSONString(input.getArgs()));
            _pendingWrites.put(obj);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void cleanup() {
        _running = false;
        _process.destroy();
        _inputs.clear();
    }

    private void handleAck(Map<String, Object> action) {
        String id = (String) action.get("id");
        WorkMessage acked = _inputs.remove(id);
        if (acked == null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(Map<String, Object> action) {
        String id = (String) action.get("id");
        WorkMessage failed = _inputs.remove(id);
        if (failed == null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleError(Map<String, Object> action) {
        String msg = (String) action.get("msg");
        _collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    private void die(Throwable exception) {
        _exception = exception;
    }
}
