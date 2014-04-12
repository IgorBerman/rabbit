package worker;

public class WorkerConf {

    private String codeDir;
    private String PIDDir;

    public WorkerConf() {
        super();
    }

    public WorkerConf(String codeDir, String pIDDir) {
        super();
        this.codeDir = codeDir;
        PIDDir = pIDDir;
    }

    public String getCodeDir() {
        return codeDir;
    }

    public void setCodeDir(String codeDir) {
        this.codeDir = codeDir;
    }

    public String getPIDDir() {
        return PIDDir;
    }

    public void setPIDDir(String pIDDir) {
        PIDDir = pIDDir;
    }

}
