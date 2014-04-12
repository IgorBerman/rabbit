package worker;

import java.util.List;


public class WorkMessage {
    private String task;
    private List<String> args;

    public WorkMessage(String task, List<String> args) {
        super();
        this.task = task;
        this.args = args;
    }

    public WorkMessage() {
        super();
    }

    public void setTask(String task) {
        this.task = task;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String getTask() {
        return task;
    }

    public List<String> getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return task + args;
    }
}
