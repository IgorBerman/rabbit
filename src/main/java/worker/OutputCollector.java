package worker;



public interface OutputCollector {

    public void ack(WorkMessage acked);

    public void fail(WorkMessage failed);

    public void reportError(Exception exception);
}
