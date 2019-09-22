package pt.isel.ngspipes.engine_executor.implementations;

import org.apache.logging.log4j.LogManager;
import pt.isel.ngspipes.engine_common.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Output;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import pt.isel.ngspipes.engine_common.utils.CommandBuilderSupplier;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_common.utils.JacksonUtils;
import pt.isel.ngspipes.engine_executor.entities.VagrantConfig;
import pt.isel.ngspipes.engine_executor.entities.VagrantSshConfig;
import pt.isel.ngspipes.engine_executor.utils.ProcessRunner;

import java.io.*;

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
        finishVagrant(pipeline);
        pipeline.getState().setState(StateEnum.SUCCESS);
        reporter.reportInfo("Pipeline Finished Successfully");
    }

    @Override
    void finishOnError(Pipeline pipeline, Exception ex) throws ExecutorException, ProgressReporterException {
        finishVagrant(pipeline);
        ExecutionState failed = new ExecutionState(StateEnum.FAILED, ex);
        pipeline.setState(failed);
        reporter.reportInfo("Pipeline Finished with Error");
    }

    @Override
    String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            String commandStr = getJobBuildedCommand(job, pipeline);
            String ssh = getSshCommand(pipeline);
            return ssh + commandStr;
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new ExecutorException("Error when building step", e);
        }
    }

    private String getJobBuildedCommand(SimpleJob job, Pipeline pipeline) throws CommandBuilderException {
        ICommandBuilder commandBuilder = CommandBuilderSupplier.getCommandBuilder(job.getExecutionContext().getContext());
        String cmdBuilt = commandBuilder.build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
        StringBuilder command = new StringBuilder("\\config ");
        command .append(TAG)
                .append(pipeline.getName())
                .append(" ")
                .append(cmdBuilt);
        String commandStr = command.toString();
        commandStr = commandStr.replace(workingDirectory, BASE_DIRECTORY).replace(File.separatorChar + "", fileSeparator);
        return commandStr;
    }

    private String getSshCommand(Pipeline pipeline) {
        StringBuilder ssh = new StringBuilder("ssh -F ");
        ssh .append(workingDirectory)
            .append(File.separatorChar)
            .append(pipeline.getName());
        return ssh.toString();
    }

    @Override
    void executeJob(String executeCmd, Job job, Pipeline pipeline) throws ExecutorException {
        try {
            int exitCode = ProcessRunner.run(executeCmd, workingDirectory + File.separatorChar + pipeline.getName(), reporter);
            System.out.println(exitCode);
        } catch (EngineCommonException e) {
            throw new ExecutorException("", e);
        }
    }

    @Override
    public void execute(Pipeline pipeline) throws ExecutorException {
        configure(pipeline);
        createConfigSsh(pipeline);
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

    private void createConfigSsh(Pipeline pipeline) throws ExecutorException {
        String host = TAG + pipeline.getName();
        String machinePath = workingDirectory + File.separatorChar +
                            pipeline.getName() + File.separatorChar +
                            ".vagrant" + File.separatorChar + "machines"
                            + File.separatorChar + host;
        VagrantSshConfig vagrantSshConfig = new VagrantSshConfig(host, machinePath);
        try {
            String path = pipeline.getEnvironment().getWorkDirectory() + File.separatorChar + "config";
            IOUtils.writeFile(path, vagrantSshConfig.toString());
        } catch (IOException e) {
            throw new ExecutorException("Error creating ssh config", e);
        }
    }

    private void createAndCopyVMFiles(Pipeline pipeline) throws IOException {
        createVmConfigFile(pipeline);
        String vagrantFileName = "Vagrantfile";
        String destPath = pipeline.getEnvironment().getWorkDirectory() + fileSeparator + vagrantFileName;
        String vagrantFileContent = readVagrantFile();
        IOUtils.writeFile(destPath, vagrantFileContent);
    }

    private String readVagrantFile() {
        InputStream in = getClass().getResourceAsStream("/Vagrantfile");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        reader.lines().forEach((line) -> sb.append(line).append("\n"));
        return sb.toString();
    }

    private void initVM(Pipeline pipeline) throws ExecutorException {
        String workDirectory = workingDirectory + File.separatorChar + pipeline.getName();
        try {
            ProcessRunner.run("vagrant up", workDirectory , reporter);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error initiating vagrant machine " + tag + pipeline.getName() + ".", e);
        }
    }

    private void createVmConfigFile(Pipeline pipeline) throws IOException {
        String workDirectory = pipeline.getEnvironment().getWorkDirectory();
        String vagrantConfig = getVmConfigFileContent(pipeline);
        IOUtils.writeFile(workDirectory + fileSeparator + CONFIG_NAME, vagrantConfig);
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
            String command = "vagrant destroy " + this.getClass().getSimpleName() + pipeline.getName() + " -f";
            ProcessRunner.run(command, workDirectory, reporter);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error destroying vagrant machine " + tag + pipeline.getName() + ".", e);
        }
    }

}
