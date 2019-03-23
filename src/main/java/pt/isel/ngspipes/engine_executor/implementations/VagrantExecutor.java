package pt.isel.ngspipes.engine_executor.implementations;

import pt.isel.ngspipes.engine_executor.entities.VagrantConfig;
import pt.isel.ngspipes.engine_executor.utils.ProcessRunner;
import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import org.apache.logging.log4j.LogManager;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_common.utils.JacksonUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

public class VagrantExecutor extends LocalExecutor {

    private static final String TAG = "VagrantExecutor";
    private static final String FILE_SEPARATOR = "/";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
                                                File.separatorChar + "Engine";
    private static final String HOST_IP_PREFIX = "10.141.141.";
    private static final String CONFIG_NAME = "config.json";
    private static final String BASE_DIRECTORY = "/home/vagrant";


    private int ipAddressSuffix = 14;

    public VagrantExecutor(IExecutionProgressReporter reporter, String workingDirectory) {
        super(LogManager.getLogger(VagrantExecutor.class.getName()), TAG, FILE_SEPARATOR, reporter, workingDirectory);
    }

    public VagrantExecutor(IExecutionProgressReporter reporter) {
        this(reporter, WORK_DIRECTORY);
    }

    @Override
    void finishSuccessfully(Pipeline pipeline) throws ExecutorException, ProgressReporterException {
        pipeline.getState().setState(StateEnum.SUCCESS);
        reporter.reportInfo("Pipeline Finished Successfully");
        finishVagrant(pipeline);
    }

    @Override
    void finishOnError(Pipeline pipeline) throws ExecutorException, ProgressReporterException {
        pipeline.getState().setState(StateEnum.FAILED);
        reporter.reportInfo("Pipeline Finished with Error");
        finishVagrant(pipeline);
    }

    @Override
    String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            String cmdBuilded = job.getCommandBuilder().build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
            StringBuilder command = new StringBuilder("vagrant ssh ");
            command.append(TAG)
                    .append(pipeline.getName())
                    .append(" -c \"")
                    .append(cmdBuilded)
                    .append("\"");
            String commandStr = command.toString();
            return commandStr.replace(workingDirectory, BASE_DIRECTORY).replace(File.separatorChar + "", fileSeparator);
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new ExecutorException("Error when building step", e);
        }
    }

    @Override
    void executeJob(String executeCmd, Job job, Pipeline pipeline) throws ExecutorException {
        try {
            ProcessRunner.runOnSpecificFolder(executeCmd, reporter, workingDirectory + File.separatorChar + pipeline.getName());
        } catch (EngineCommonException e) {
            throw new ExecutorException("", e);
        }
    }

    @Override
    public void execute(Pipeline pipeline) throws ExecutorException {
        configure(pipeline);
        copyPipelineInputs(pipeline);
        run(pipeline, pipeline.getGraph());
    }



    private void configure(Pipeline pipeline) throws ExecutorException {
        logger.info("Configuring " + TAG);
        try {
//            if (!exist) {
            ipAddressSuffix++;
            createAndCopyVMFiles(pipeline);
            initVM(pipeline);
//            }
        } catch (IOException e) {
            throw new ExecutorException("Error initiating engine", e);
        }
    }

    private void createAndCopyVMFiles(Pipeline pipeline) throws IOException {
        createVmConfigFile(pipeline);
        String vagrantFileName = "Vagrantfile";
        File source = getVagrantFile(vagrantFileName);
        String destPath = pipeline.getEnvironment().getWorkDirectory() + fileSeparator + vagrantFileName;
        Files.copy(Paths.get(source.getPath()), Paths.get(destPath), StandardCopyOption.REPLACE_EXISTING);
    }

    private void initVM(Pipeline pipeline) throws ExecutorException {
        String workDirectory = workingDirectory + File.separatorChar + pipeline.getName();
        try {
            ProcessRunner.runOnSpecificFolder("vagrant up" , reporter, workDirectory);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error initiating vagrant machine " + tag + pipeline.getName() + ".", e);
        }
    }

    private void createVmConfigFile(Pipeline pipeline) throws IOException {
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        String vagrantConfig = getVmConfigFileContent(pipeline);
        IOUtils.writeFile(workDirectory + fileSeparator + CONFIG_NAME, vagrantConfig);
    }

    private File getVagrantFile(String vagrantFileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(vagrantFileName).getFile());
        return file;
    }

    private String getVmConfigFileContent(Pipeline pipeline) throws IOException {
        String ip_address = HOST_IP_PREFIX + ipAddressSuffix;
        Environment env = pipeline.getEnvironment();
        int cpu = env.getCpu();
        cpu = cpu == 0 ? 1 : cpu;
        String name = pipeline.getName();
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        VagrantConfig vagrantConfig = new VagrantConfig(ip_address, TAG + name, workDirectory, env.getMemory(), cpu, name);
        return JacksonUtils.serialize(vagrantConfig);
    }

    private void finishVagrant(Pipeline pipeline) throws ExecutorException {
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        try {
            String command = "vagrant destroy " + this.getClass().getName() + pipeline.getName() + " -f";
            ProcessRunner.runOnSpecificFolder(command, reporter, workDirectory);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error destroying vagrant machine " + tag + pipeline.getName() + ".", e);
        }
    }

}
