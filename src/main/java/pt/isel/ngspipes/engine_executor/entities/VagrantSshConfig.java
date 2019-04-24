package pt.isel.ngspipes.engine_executor.entities;

import java.io.File;

public class VagrantSshConfig {

    private final String host;
    private final String hostname = "Hostname 127.0.0.1";
    private final String user = "User vagrant";
    private final String port = "Port 2222";
    private final String userKnownHostFile = "UserKnownHostsFile /dev/null";
    private final String strictHostKeyChecking = "StrictHostKeyChecking no";
    private final String passwordAuthentication = "PasswordAuthentication no";
    private final String identityFile;
    private final String identitiesOnly = "IdentitiesOnly yes";
    private final String logLevel = "LogLevel FATAL";

    public VagrantSshConfig(String host, String machinePath) {
        this.host = "Host " + host;
        this.identityFile = "IdentityFile " + machinePath + File.separatorChar +
                            "virtualbox" + File.separatorChar + "private_key";
    }

    @Override
    public String toString() {
        StringBuilder toString = new StringBuilder();
        toString.append(host)
                .append(System.getProperty("line.separator"))
                .append(hostname)
                .append(System.getProperty("line.separator"))
                .append(user)
                .append(System.getProperty("line.separator"))
                .append(port)
                .append(System.getProperty("line.separator"))
                .append(userKnownHostFile)
                .append(System.getProperty("line.separator"))
                .append(strictHostKeyChecking)
                .append(System.getProperty("line.separator"))
                .append(passwordAuthentication)
                .append(System.getProperty("line.separator"))
                .append(identityFile)
                .append(System.getProperty("line.separator"))
                .append(identitiesOnly)
                .append(System.getProperty("line.separator"))
                .append(logLevel);
        return toString.toString();
    }
}
