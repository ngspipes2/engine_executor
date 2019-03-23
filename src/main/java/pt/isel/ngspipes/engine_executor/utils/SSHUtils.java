package pt.isel.ngspipes.engine_executor.utils;

import com.jcraft.jsch.*;

import java.io.*;
import java.util.Properties;

public class SSHUtils {

    public static ChannelSftp getChannelSftp(SSHConfig sshConfig) throws JSchException {
        Session session = getSessionByConfig(sshConfig);
        ChannelSftp channelSftp;

        channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        return channelSftp;
    }

    public static void upload(String base_dir, String dest, String source, ChannelSftp sftp, String fileSeparator) throws SftpException, FileNotFoundException, UnsupportedEncodingException {
        File file = new File(source);
        createIfNotExist(base_dir, dest, sftp, fileSeparator);
        sftp.cd(dest);
        System.out.println("directory: " + dest);
        if(file.isFile()){
            InputStream ins = new FileInputStream(file);
            sftp.put(ins, new String(file.getName().getBytes(),"UTF-8"));
            sftp.chmod(511, dest + fileSeparator + file.getName());
        } else if(file.isDirectory() || file.listFiles() == null) {
            createFolder(file.getPath(), sftp);
        } else {
            File[] files = file.listFiles();
            for (File file2 : files) {
                String dir = file2.getAbsolutePath();
                if(file2.isDirectory()){
                    String str = dir.substring(dir.lastIndexOf(fileSeparator));
                    dest = dest + str;
                }
                System.out.println("directory is :" + dest);
                upload(base_dir, dest, source + dir, sftp, fileSeparator);
            }
        }
    }

    public static void copy(String base_dir, String source, String dest, String inputName, SSHConfig config, String fileSeparator) throws JSchException, InterruptedException, SftpException {
        ChannelSftp sftp = null;
        try {
            sftp = SSHUtils.getChannelSftp(config);
            createIfNotExist(base_dir, dest, sftp, fileSeparator);
            String cpyCmd = "cp -R " + source + inputName + " " + dest;
            Session session = sftp.getSession();
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(cpyCmd);
            channel.connect();
            while(channel.isConnected()) {
                Thread.sleep(20);
            }
            int status = channel.getExitStatus();
            if(status != 0)
                throw new JSchException("Error copying input file " + inputName);

        } finally {
            if(sftp != null) {
                sftp.disconnect();
            }
        }
    }

    public static void createIfNotExist(String base_dir, String folderPath, ChannelSftp sftp, String fileSeparator) throws SftpException {
        sftp.cd(base_dir);
        String[] folders = folderPath.split(fileSeparator);
        for ( String folder : folders ) {
            if (folder.isEmpty() || base_dir.contains(folder))
                continue;
            createFolder(folder, sftp);
        }
        sftp.chmod(511, folderPath);
    }

    private static void createFolder(String folderPath, ChannelSftp sftp) throws SftpException {
        try {
            sftp.cd(folderPath);
        } catch (SftpException e) {
            sftp.mkdir(folderPath);
            sftp.cd(folderPath);
            System.out.println("mkdir:" + folderPath);
        }
    }

    private static Session getSessionByConfig(SSHConfig sshConfig) throws JSchException {
        Session session;
        if (sshConfig.getKeyPath() == null || sshConfig.getKeyPath().isEmpty())
            session = getSession(sshConfig);
        else
            session = getSessionWithKey(sshConfig);

        Properties conf = new Properties();
        conf.put("StrictHostKeyChecking", "no");
        session.setConfig(conf);
        session.connect();

        return session;
    }

    private static Session getSession(SSHConfig config) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
        session.setPassword(config.getPassword());
        session.setConfig("StrictHostKeyChecking", "no"); // new
        return session;
    }

    private static Session getSessionWithKey(SSHConfig config) throws JSchException {
        JSch jsch = new JSch();
        jsch.addIdentity(config.getKeyPath(), config.getPassword());
        Session session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
        session.setPassword(config.getPassword());
        session.setConfig("StrictHostKeyChecking", "no"); // new

        return session;
    }

}
