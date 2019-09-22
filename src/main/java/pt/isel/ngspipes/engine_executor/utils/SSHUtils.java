package pt.isel.ngspipes.engine_executor.utils;

import com.jcraft.jsch.*;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SSHUtils {

    static Collection<String> IGNORE_FILES = new LinkedList<>(Arrays.asList("." , ".."));

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
            uploadFile(dest, sftp, fileSeparator, file);
        } else if(file.isDirectory() || file.listFiles() == null) {
            createFolder(file.getPath(), sftp);
        } else {
            uploadDirectoryFiles(base_dir, dest, source, sftp, fileSeparator, file);
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

    public static void download(String base_dir, String dest, String fileName, ChannelSftp sftp, String type) throws SftpException, IOException {
        sftp.cd(base_dir);
        String directory = "directory";
        if (type.equalsIgnoreCase(directory)) {
            downloadDirectory(dest, fileName, sftp, directory);
        } else if (type.equalsIgnoreCase("file")) {
            downloadFile(dest, fileName, sftp);
        } else if (type.equalsIgnoreCase("file[]")) {
            downloadListOfFiles(base_dir, dest, fileName, sftp);
        }
    }

    public static List<String> getFilesNameByPattern(ChannelSftp channelSftp, String pattern, String filesPath) throws SftpException {
        List<String> filesName = new LinkedList<>();
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        Vector filelist = channelSftp.ls(filesPath);
        for(int i=0; i< filelist.size(); i++){
            ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) filelist.get(i);
            String filename = entry.getFilename();
            Path path = Paths.get(entry.getFilename());
            if (!IGNORE_FILES.contains(filename) && matcher.matches(path))
                filesName.add(filename);
        }
        return filesName;
    }


    private static void downloadListOfFiles(String base_dir, String dest, String source, ChannelSftp sftp) throws SftpException, IOException {
        Vector fileList = sftp.ls(base_dir);
        Pattern pattern = Pattern.compile(source);
        for(int i=0; i< fileList.size(); i++) {
            ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) fileList.get(i);
            String filename = entry.getFilename();
            Matcher m = pattern.matcher(filename);
            if (IGNORE_FILES.contains(filename) || !m.matches())
                continue;

            downloadFile(dest, filename, sftp);
        }
    }

    private static void downloadDirectory(String dest, String filename, ChannelSftp sftp, String directory) throws SftpException, IOException {
        Vector filelist = sftp.ls(filename);
        for(int i=0; i< filelist.size(); i++) {
            ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) filelist.get(i);
            String subFilename = entry.getFilename();
            if (IGNORE_FILES.contains(subFilename))
                continue;
            else if (entry.getAttrs().isDir()) {
                File destFile = new File(dest + File.separatorChar + subFilename);
                destFile.mkdirs();
                download(dest, subFilename, subFilename, sftp, directory);
            } else {
                String lpwd = sftp.pwd();
                if (!lpwd.contains(filename)) {
                    sftp.cd(filename);
                    File directoryFile = new File(dest + File.separatorChar + filename);
                    directoryFile.mkdirs();
                }
                downloadFile(dest + File.separatorChar + filename, subFilename, sftp);
            }
        }
    }

    private static void uploadDirectoryFiles(String base_dir, String dest, String source, ChannelSftp sftp, String fileSeparator, File file) throws SftpException, FileNotFoundException, UnsupportedEncodingException {
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

    private static void uploadFile(String dest, ChannelSftp sftp, String fileSeparator, File file) throws FileNotFoundException, SftpException, UnsupportedEncodingException {
        InputStream ins = new FileInputStream(file);
        sftp.put(ins, new String(file.getName().getBytes(),"UTF-8"));
        sftp.chmod(511, dest + fileSeparator + file.getName());
    }

    private static void downloadFile(String dest, String filename, ChannelSftp sftp) throws SftpException, IOException {
        BufferedInputStream bis = new BufferedInputStream(sftp.get(filename));
        File destFile = new File(dest + File.separatorChar + filename);
        OutputStream os = new FileOutputStream(destFile);
        BufferedOutputStream bos = new BufferedOutputStream(os);
        byte[] buffer = new byte[1024];
        int readCount;
        while ((readCount = bis.read(buffer)) > 0) {
            bos.write(buffer, 0, readCount);
        }
        bis.close();
        bos.close();
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
