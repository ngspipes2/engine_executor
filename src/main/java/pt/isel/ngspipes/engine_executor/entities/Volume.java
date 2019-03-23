package pt.isel.ngspipes.engine_executor.entities;

public class Volume {

    String containerPath;
    String hostPath;
    String mode;

    public Volume(String containerPath, String hostPath, String mode) {
        this.containerPath = containerPath;
        this.hostPath = hostPath;
        this.mode = mode;
    }

    public Volume(String containerPath, String hostPath) {
        this();
        this.containerPath = containerPath;
        this.hostPath = hostPath;
    }

    public Volume() {
        this.mode = "RW";
    }

    public String getContainerPath() { return containerPath; }
    public void setContainerPath(String containerPath) { this.containerPath = containerPath; }

    public String getHostPath() { return hostPath; }
    public void setHostPath(String hostPath) { this.hostPath = hostPath; }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }

}
