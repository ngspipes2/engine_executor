package pt.isel.ngspipes.engine_executor.utils;

public class SSHConfig {

    private final String user;
    private final String pass;
    private String keyPath;
    private final String host;
    private final int port;

    public SSHConfig(String user, String pass, String host, int port) {
        this.user = user;
        this.pass = pass;
        this.host = host;
        this.port = port;
    }

    public SSHConfig(String user, String pass, String keyPath, String host, int port) {
        this.user = user;
        this.pass = pass;
        this.keyPath = keyPath;
        this.host = host;
        this.port = port;
    }

    public String getUser() { return user; }
    public String getPassword() { return pass; }
    public String getKeyPath() { return keyPath; }
    public String getHost() { return host; }
    public int getPort() { return port; }
}
