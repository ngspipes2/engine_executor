package pt.isel.ngspipes.engine_executor.utils;

import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;

public class ProcessRunner {

    private static final Logger logger = Logger.getLogger(ProcessRunner.class);

    private static class ExceptionBox{
        Exception ex;
    }

    @FunctionalInterface
    private interface LogLambda {
        void log() throws Exception;
    }

    @FunctionalInterface
    private interface InternalReporter {
        void report(String msg) throws ProgressReporterException;
    }

    private static void logStream(InputStream in, InternalReporter reporter) throws IOException, ProgressReporterException {

        String line;
        StringBuilder sb = new StringBuilder();

        try (BufferedReader bf = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")))) {
            while ((line = bf.readLine()) != null) {
                sb.append(line).append("\n");
                reporter.report(line);
            }
        } finally {
//            if (sb.length() != 0)
//                logger.error(sb.toString());

        }
    }

    public static void runOnSpecificFolder(String command, IExecutionProgressReporter reporter, String workDirectory) throws EngineCommonException {
        ExceptionBox inputBox = new ExceptionBox();
        ExceptionBox errorBox = new ExceptionBox();
        Process p;

        try {
            logger.trace("Executing command: " + command);
            p = Runtime.getRuntime().exec(command, null, new File(workDirectory));

            Thread inputThread = createThread(() -> logStream(p.getInputStream(), reporter::reportInfo), inputBox);
            Thread errorThread = createThread(() -> logStream(p.getErrorStream(), reporter::reportInfo), errorBox);

            inputThread.join();
            errorThread.join();
        } catch (Exception ex) {
            try {
                reporter.reportInfo(ex.getMessage());
            } catch (ProgressReporterException e) {
                e.printStackTrace();
            }
            throw new EngineCommonException("Error executing command " + command, ex);
        }
    }

    public static void run(String command, String workingDirectory, IExecutionProgressReporter reporter) throws EngineCommonException {
        ExceptionBox inputBox = new ExceptionBox();
        ExceptionBox errorBox = new ExceptionBox();
        Process p;

        try{

            logger.trace("Executing command: " + command);
            if (workingDirectory != null && !workingDirectory.isEmpty())
                p = Runtime.getRuntime().exec(command, null, new File(workingDirectory));
            else
                p = Runtime.getRuntime().exec(command);

            Thread inputThread = createThread(()->logStream(p.getInputStream(), reporter::reportInfo), inputBox);
            Thread errorThread = createThread(()->logStream(p.getErrorStream(), reporter::reportInfo), errorBox);

            inputThread.join();
            errorThread.join();
        }catch(Exception ex){
            try {
                reporter.reportInfo(ex.getMessage());
            } catch (ProgressReporterException e) {
                e.printStackTrace();
            }
            throw new EngineCommonException("Error executing command " + command, ex);
        }

        //Ignoring IOExceptions from logStream(InputStream in)
        try {
            int exitCode = p.waitFor();

            String message = "Command " + command + " finished with Exit Code = " + exitCode;
            logger.trace(message);
            reporter.reportInfo(message);
        } catch (Exception ex) {
            throw new EngineCommonException("Error executing command " + command, ex);
        }
    }

    private static Thread createThread(LogLambda action,  ExceptionBox box) {
        Thread t = new Thread( () -> {
            try {
                action.log();
            } catch(Exception e) {
                box.ex = e;
            }
        });

        t.setDaemon(true);
        t.start();

        return t;
    }
}
