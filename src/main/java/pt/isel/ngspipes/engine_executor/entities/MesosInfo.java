package pt.isel.ngspipes.engine_executor.entities;

public class MesosInfo {

    private final String  sshHost;
    private final String  sshUser;
    private final String  sshPassword;
    private final int  sshPort;
    private final String chronosEndPoint;
    private final String baseDirectory;
    private String  keyPath;

    public MesosInfo(String sshHost, String sshUser, String sshPassword, int sshPort, String chronosEndPoint, String baseDirectory, String keyPath) {
        this(sshHost, sshUser, sshPassword, sshPort, chronosEndPoint, baseDirectory);
        this.keyPath = keyPath;
    }

    public MesosInfo(String sshHost, String sshUser, String sshPassword, int sshPort, String chronosEndPoint, String baseDirectory) {
        this.sshHost = sshHost;
        this.sshUser = sshUser;
        this.sshPassword = sshPassword;
        this.sshPort = sshPort;
        this.chronosEndPoint = chronosEndPoint;
        this.baseDirectory = baseDirectory;
    }

    public String getSshHost() { return sshHost; }
    public String getSshUser() { return sshUser; }
    public String getSshPassword() { return sshPassword; }
    public int getSshPort() { return sshPort; }
    public String getChronosEndPoint() { return chronosEndPoint; }
    public String getBaseDirectory() { return baseDirectory; }
    public String getKeyPath() { return keyPath; }

}
