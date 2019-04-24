package pt.isel.ngspipes.engine_executor.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.ExecutionNode;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import pt.isel.ngspipes.engine_common.interfaces.IExecutor;
import pt.isel.ngspipes.engine_common.utils.*;
import pt.isel.ngspipes.engine_executor.entities.*;
import pt.isel.ngspipes.engine_executor.entities.factory.ChronosJobStatusFactory;
import pt.isel.ngspipes.engine_executor.utils.HttpUtils;
import pt.isel.ngspipes.engine_executor.utils.SSHConfig;
import pt.isel.ngspipes.engine_executor.utils.SSHUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MesosExecutor implements IExecutor {

    private static final String TAG = "MesosEngine";
    private static final String CHRONOS_DEPENDENCY = "scheduler/dependency";
    private static final String CHRONOS_ISO = "scheduler/iso8601";
    private static final String CHRONOS_JOB = "scheduler/jobs";
    private static final String CHRONOS_JOB_SEARCH = "scheduler/jobs/search?name=";
    private static final String EPSILON = "P1Y12M12D";
    private static final String FILE_SEPARATOR = "/";

    private ChannelSftp channelSftp;
    private final Map<String, Collection<Job>> TASKS_BY_EXEC_ID = new HashMap<>();
    final Map<String, Pipeline> pipelines = new HashMap<>();
    final String workingDirectory;
    final MesosInfo mesosInfo;
    final Logger logger = LogManager.getLogger(MesosExecutor.class.getName());
    final IExecutionProgressReporter reporter;

    public MesosExecutor(IExecutionProgressReporter reporter, MesosInfo mesosInfo, String workingDirectory) {
        this.mesosInfo = mesosInfo;
        this.workingDirectory = workingDirectory;
        this.reporter = reporter;
    }

    public MesosExecutor(IExecutionProgressReporter reporter, MesosInfo mesosInfo) {
        this(reporter, mesosInfo, mesosInfo.getBaseDirectory());
    }

    @Override
    public void execute(Pipeline pipeline) throws ExecutorException {
        copyPipelineInputs(pipeline);
        run(pipeline, pipeline.getGraph());
    }

    @Override
    public boolean stop(String executionId) throws ExecutorException {
        return false;
    }

    @Override
    public boolean clean(String executionId) throws ExecutorException {
        return false;
    }

    @Override
    public boolean cleanAll() throws ExecutorException {
        return false;
    }

    @Override
    public void getPipelineOutputs(Pipeline pipeline, String outputDirectory) throws ExecutorException {
        for (Output output : pipeline.getOutputs()) {
            getPipelineOutput(output, pipeline, outputDirectory);
        }
    }

    @Override
    public String getWorkingDirectory() {  return workingDirectory; }

    @Override
    public String getFileSeparator() {
        return FILE_SEPARATOR;
    }


    private void copyPipelineInputs(Pipeline pipeline) throws ExecutorException {
        logger.trace(TAG + ":: Copying pipeline " + pipeline.getName() + " inputs.");

        updateEnvironment(pipeline.getEnvironment());
        ChannelSftp sftp = null;
        try {
            SSHConfig config = getSshConfig();
            sftp = SSHUtils.getChannelSftp(config);
            SSHUtils.createIfNotExist(mesosInfo.getBaseDirectory(), pipeline.getEnvironment().getWorkDirectory(), sftp, FILE_SEPARATOR);
        } catch (JSchException | SftpException e) {
            throw new ExecutorException("Error connecting server " + mesosInfo.getSshHost());
        } finally {
            if(sftp != null) {
                sftp.disconnect();
                try {
                    sftp.getSession().disconnect();
                } catch (JSchException e) {
                    throw new ExecutorException("Error copying inputs.", e);
                }
            }
        }

        for (Job job : pipeline.getJobs()) {
            updateEnvironment(job.getEnvironment());
            uploadInputs(pipeline.getName(), job, job.getInputs());
        }
    }

    private void run(Pipeline pipeline, Collection<ExecutionNode> executionGraph) {
        try {
            List<Job> executionJobs = new LinkedList<>();
            storeJobs(pipeline, executionJobs);
            sortByExecutionOrder(executionGraph, pipeline, executionJobs);
            execute(pipeline, executionJobs);
            scheduleFinishPipeline(pipeline, executionJobs);
        } catch (ExecutorException | ProgressReporterException | IOException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(pipeline.getName(), state);
        }
    }

    private void storeJobs(Pipeline pipeline, List<Job> executionJobs) {
        if (!TASKS_BY_EXEC_ID.containsKey(pipeline.getName()))
            TASKS_BY_EXEC_ID.put(pipeline.getName(), executionJobs);
        else
            TASKS_BY_EXEC_ID.get(pipeline.getName()).addAll(executionJobs);
    }

    private void sortByExecutionOrder(Collection<ExecutionNode> executionGraph, Pipeline pipeline, List<Job> executionJobs) {
        executionGraph.forEach((node) -> executionJobs.add(node.getJob()));
        for (ExecutionNode node : executionGraph) {
            addByDependency(pipeline, executionJobs, node);
        }
    }

    private void execute(Pipeline pipeline, List<Job> executionJobs) throws ExecutorException {
        try {
            ValidateUtils.validatePipelineState(pipeline);

            for (Job job : executionJobs) {
                ValidateUtils.validateResources(job, pipeline);
                SimpleJob sJob = (SimpleJob) job;
                copyChainInputs(sJob, pipeline);
                run(sJob, getChronosJob(sJob, pipeline), pipeline);
            }
        } catch(EngineCommonException e) {
            throw new ExecutorException("Error validating pipeline steps.", e);
        }
    }

    private void scheduleFinishPipeline(Pipeline pipeline, List<Job> executionJobs) throws IOException, ProgressReporterException {
        String jobId = "success" + pipeline.getName();
        String successJob = getSuccessChronosJob(executionJobs, jobId, pipeline.getName());
        HttpUtils.post(mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY, successJob);
        waitUntilJobFinish(new String[] {jobId, pipeline.getName()});
        pipeline.getState().setState(StateEnum.SUCCESS);
        reporter.reportInfo("Pipeline Finished");
    }

    private void getPipelineOutput(Output output, Pipeline pipeline, String outputDirectory) throws ExecutorException {
        Job outputJob = pipeline.getJobById(output.getOriginJob());
        try {
            String outVal = output.getValue().toString();
            String outputsDirectory = outputJob.getEnvironment().getOutputsDirectory();
            String dest = outputDirectory + File.separatorChar;
            SSHUtils.download(outputsDirectory, dest, outVal, channelSftp, output.getType());
        } catch (IOException | SftpException e) {
            throw new ExecutorException("Error getting pipeline " + output.getName() + " output.", e);
        }
    }

    private void run(SimpleJob job, String chronosJob, Pipeline pipeline) throws ExecutorException {
        try {
            reporter.reportInfo("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName());
            SSHUtils.createIfNotExist(workingDirectory, job.getEnvironment().getOutputsDirectory(), channelSftp, FILE_SEPARATOR);
            String url = mesosInfo.getChronosEndPoint() + CHRONOS_ISO;
            if (!job.getParents().isEmpty())
                url = mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY;
            HttpUtils.post(url, chronosJob);
            String jobId = pipeline.getName() + "_" + job.getId();
            validateOutputs(job, pipeline.getName());
            runInconclusiveDependencies(job, pipeline, this::waitUntilJobFinish, jobId, pipeline.getName());
        } catch (IOException | SftpException | ProgressReporterException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            throw new ExecutorException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private void validateOutputs(SimpleJob stepCtx, String executionId) throws ExecutorException {
        StringBuilder chronosJobCmd = new StringBuilder();
        String outputDir = stepCtx.getEnvironment().getOutputsDirectory();

        for (Output outCtx : stepCtx.getOutputs()) {
            String type = outCtx.getType();
            if (type.contains("ile") || type.contains("irectory")) {
                String out = outCtx.getValue() + "";
                if(!out.isEmpty()) {
                    if (chronosJobCmd.length() != 0)
                        chronosJobCmd.append(" && ");
                    chronosJobCmd.append("find ")
                            .append(outputDir + FILE_SEPARATOR + out);
                }
            }
        }
//        chronosJobCmd.append("&& chmod -R 777 ").append(outputDir);
        try {
            String parent = executionId + "_" + stepCtx.getId();
            String id = "validateOutputs";
            String chronosJob = getDependentChronosJob(chronosJobCmd.toString(), Collections.singletonList(parent), id + parent);
            HttpUtils.post(mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY, chronosJob);
        } catch (IOException e) {
            logger.error("Error step: " + stepCtx.getId() + " didn't produces all outputs." , e);
            throw new ExecutorException("Error step: " + stepCtx.getId() + " didn't produces all outputs." , e);
        }
    }

    private void runInconclusiveDependencies(SimpleJob job, Pipeline pipeline, Consumer<String[]> wait, String id, String pipelineName) {
        if (job.isInconclusive()) {
            job.setInconclusive(false);
            TaskFactory.createAndStart("inconclusive" + pipeline.getName(), () -> {
                wait.accept(new String[]{id, pipelineName});
                Collection<ExecutionNode> graph = TopologicSorter.parallelSort(pipeline, job);
                try {
                    for (ExecutionNode node : graph) {
                        SimpleJob childJob = (SimpleJob) node.getJob();
                        if (childJob.getSpread() != null) {
                            SpreadJobExpander.expandSpreadJob(pipeline, childJob, graph, this::getOutputValues, FILE_SEPARATOR);
                        }
                    }
                    run(pipeline, graph);
                } catch (EngineCommonException e) {
                    throw new ExecutorException("Error running job " + job.getId() + ".", e);
                }
            });
        }
    }

    private List<String> getOutputValues(String outputName, Job job) {
        if (job.getSpread() != null) {
            return getOutputValuesFromSpreadJob(outputName, job, "");
        } else {
            return getOutputValuesFromJob(outputName, job);
        }
    }

    private List<String> getOutputValuesFromJob(String chainOutput, Job originJob) {
        throw new NotImplementedException();
    }

    private List<String> getOutputValuesFromSpreadJob(String chainOutput, Job originJob, String spreadId) {
        throw new NotImplementedException();
    }

    private String getSuccessChronosJob(List<Job> executionJobs, String jobId, String pipelineName) throws IOException {
        List<String> parents = executionJobs.stream().map((job)-> "validateOutputs" + pipelineName + "_" + job.getId()).collect(Collectors.toList());
        return getDependentChronosJob("ls", parents, jobId);
    }

    private void waitUntilJobFinish(String[] ids) {
        String url = mesosInfo.getChronosEndPoint() + CHRONOS_JOB_SEARCH + ids[0];
        ChronosJobStatusDto chronosJobStatusDto = null;
        try {
            do {
                String content = HttpUtils.get(url);
                chronosJobStatusDto = ChronosJobStatusFactory.getChronosJobStatusDto(content);
                Thread.sleep(5000);
            } while (chronosJobStatusDto.successCount <= 0);
        } catch (IOException | InterruptedException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(ids[1], state);
        }
    }

    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void addByDependency(Pipeline pipeline, List<Job> executionJobs, ExecutionNode node) {
        for (ExecutionNode child : node.getChilds()) {
            List<Job> parents = new LinkedList<>();
            Job job = child.getJob();
            if (executionJobs.contains(job))
                continue;
            job.getParents().forEach((parent) -> {
                Job parentJobById = pipeline.getJobById(parent);
                if (!parents.contains(parentJobById))
                    parents.add(parentJobById);
            });
            int present = (int) parents.stream().filter(executionJobs::contains).count();
            if (present == parents.size()) {
                executionJobs.add(job);
                addByDependency(pipeline, executionJobs, child);
            }
        }
    }

    private void copyChainInputs(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        String destDir = job.getEnvironment().getWorkDirectory() + FILE_SEPARATOR;
        String jobName = pipeline.getName() + "_" +  job.getId() + "_cp_chains";
        StringBuilder command = new StringBuilder();
        List<String> parents = new LinkedList<>();
        boolean first = true;

        for (Input inputCtx : job.getInputs()) {
            if (!inputCtx.getOriginStep().equals(job.getId())) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + FILE_SEPARATOR;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();

                if (first) {
                    first = false;
                } else {
                    command.append(" && ");
                }

                parents.add("validateOutputs" + pipeline.getName() + "_" + chainStep.getId());
                if (usedBy != null) {
                    for (String dependent : usedBy) {
                        Output outputCtx = chainStep.getOutputById(dependent);
                        String value = outputCtx.getValue().toString();
                        command.append(getCopyInputCommand(destDir, outDir, value));
                    }
                } else {
                    String value = outCtx.getValue().toString();
                    command.append(getCopyInputCommand(destDir, outDir, value));
                }
            }
        }
        if (command.length() > 0)
            getCopyChainInputsChronosJob(jobName, command.toString(), parents);
    }

    private String getChronosJob(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        String executeCmd = getExecutionCommand(job, pipeline) + "; ls";
        return getChronosJob(executeCmd, job, pipeline.getName());
    }

    private String  getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            String command = CommandBuilderSupplier.getCommandBuilder("Local").build(pipeline, job, FILE_SEPARATOR, job.getExecutionContext().getConfig());
            return command;
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new ExecutorException("Error when building step", e);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId) throws ExecutorException {
        String jobDir = job.getEnvironment().getOutputsDirectory();
        try {
            if (job.getParents().isEmpty())
                return getChronosJob(executeCmd, job, executionId, jobDir);
            else
                return getDependentChronosJob(executeCmd, job, executionId, jobDir);
        } catch (IOException ex) {
            throw new ExecutorException("Error creating chronos job.", ex);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir) throws ExecutorException, IOException {
        String schedule = "R1//P1Y";
        String command = "cd " + jobDir + " && ls && " + executeCmd;
        String name = executionId + "_" + job.getId();
        if (job.getExecutionContext().getContext().equalsIgnoreCase("DOCKER")) {
            Container container = getContainerInfo(job);
            DockerChronosJob dockerChronosJob = new DockerChronosJob(schedule, EPSILON, getCpus(job), command, name, getMemory(job), container);
            return JacksonUtils.serialize(dockerChronosJob);
        } else  {
            ChronosJob chronosJob = new ChronosJob(schedule, EPSILON, getCpus(job), command, name, getMemory(job));
            return JacksonUtils.serialize(chronosJob);
        }
    }

    private String getDependentChronosJob(String executeCmd, SimpleJob job, String executionId, String jobDir) throws ExecutorException, IOException {
        String command = "mkdir -p " + jobDir + " && " + "cd " + jobDir + " && " + executeCmd;
        String name = executionId + "_" + job.getId();
        List<String> parents = getParents(job, executionId);
        if (job.getExecutionContext().getContext().equalsIgnoreCase("DOCKER")) {
            Container container = getContainerInfo(job);
            DockerChronosJob dockerChronosJob = new DockerChronosJob(parents, EPSILON, getCpus(job), command, name, getMemory(job), container);
            return JacksonUtils.serialize(dockerChronosJob);
        } else  {
            ChronosJob chronosJob = new ChronosJob(parents, EPSILON, getCpus(job), command, name, getMemory(job));
            return JacksonUtils.serialize(chronosJob);
        }
    }

    private List<String> getParents(SimpleJob job, String pipelineName) {
        List<String> parents = new LinkedList<>();
        for (String parent : job.getParents()) {
            if (!isChainJob(job.getInputs(), parent))
                parents.add("validateOutputs" + pipelineName + "_" + parent);
        }

        if (isChainJob(job.getInputs()))
            parents.add(pipelineName + "_" +  job.getId() + "_cp_chains");
        return parents;
    }

    private boolean isChainJob(List<Input> inputs) {
        for (Input input : inputs)
            if (!input.getChainOutput().isEmpty())
                return true;
        return false;
    }

    private boolean isChainJob(List<Input> inputs, String parent) {
        for (Input input : inputs)
            if (!input.getChainOutput().isEmpty() && input.getOriginStep().equals(parent))
                return true;
        return false;
    }

    private Container getContainerInfo(SimpleJob job) {
        List<Volume> volumes = new LinkedList<>();
        String paths = mesosInfo.getBaseDirectory() + FILE_SEPARATOR;
        Volume volume = new Volume(paths, paths);
        volumes.add(volume);
        String image = getDockerUri(job.getExecutionContext().getConfig());
        return new Container(image, volumes);
    }

    private String getMemory(SimpleJob job) {
        int memory = job.getEnvironment().getMemory();
        int mem = memory == 0 ? 2048 : memory;
        return mem + "";
    }

    private String getCpus(SimpleJob job) {
        int cpu = job.getEnvironment().getCpu();
        float value = cpu == 0 ? 0.5f : cpu / 10;
        return value + "";
    }

    private String getDockerUri(Map<String, Object> config) {
        String dockerUri = "";
        dockerUri = dockerUri + config.get("uri");
        if (config.containsKey("tag"))
            dockerUri = dockerUri + ":" + config.get("tag");
        return dockerUri;
    }

    private String getCopyInputCommand(String destDir, String outDir, String value) {
        String createDestDir = "mkdir -p " + destDir;
        String copyInputs = " && cp -R " + outDir + value + " " + destDir;
        String changePermissions = " && sudo chmod -R 777 " + destDir;

        return createDestDir + copyInputs + changePermissions;
    }

    private void getCopyChainInputsChronosJob(String id, String executeCmd, List<String> parents) throws ExecutorException {
        try {
            String copyTask = getDependentChronosJob(executeCmd, parents, id);
            HttpUtils.post(mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY, copyTask);
        } catch (IOException e) {
            logger.error("Error copying inputs to: " + id, e);
            throw new ExecutorException("Error copying inputs to: " + id, e);
        }
    }

    private String getDependentChronosJob(String executeCmd, List<String> parents, String name) throws IOException {
        String epsilon = "P1Y12M12D";
        String shell = "true";
        ChronosJob chronosJob = new ChronosJob(parents, epsilon, "0.5f", shell, executeCmd, name, "512");
        return JacksonUtils.serialize(chronosJob);
    }

    private void updateEnvironment(Environment environment) {
        environment.setWorkDirectory(environment.getWorkDirectory().replace("\\", FILE_SEPARATOR));
        environment.setOutputsDirectory(environment.getOutputsDirectory().replace("\\", FILE_SEPARATOR));
    }

    private SSHConfig getSshConfig() {
        if (mesosInfo.getKeyPath() == null || mesosInfo.getKeyPath().isEmpty())
           return new SSHConfig(mesosInfo.getSshUser(), mesosInfo.getSshPassword(), mesosInfo.getSshHost(), mesosInfo.getSshPort());
        return new SSHConfig(mesosInfo.getSshUser(), mesosInfo.getSshPassword(), mesosInfo.getKeyPath(), mesosInfo.getSshHost(), mesosInfo.getSshPort());

    }

    private void uploadInputs(String pipelineName, Job step, List<Input> inputs) throws ExecutorException {
        for (Input input : inputs) {
            uploadInput(pipelineName, step, input);
            uploadInputs(pipelineName, step, input.getSubInputs());
        }
    }

    private void uploadInput(String pipelineName, Job step, Input input) throws ExecutorException {
        if (input.getType().equalsIgnoreCase("file") || input.getType().equalsIgnoreCase("directory")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                uploadInput(pipelineName, input, step);
            }
        }
    }

    private void uploadInput(String pipelineName, Input input, Job stepCtx) throws ExecutorException {
        String value = input.getValue();
        String inputName = value.substring(value.lastIndexOf(FILE_SEPARATOR) + 1);
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + FILE_SEPARATOR;
        try {
            SSHConfig config = getSshConfig();
            if (channelSftp == null)
                channelSftp = SSHUtils.getChannelSftp(config);

            String pipeline_dir = mesosInfo.getBaseDirectory() + FILE_SEPARATOR + pipelineName;
            SSHUtils.upload(pipeline_dir, destInput, value, channelSftp, FILE_SEPARATOR);
            input.setValue(pipeline_dir + FILE_SEPARATOR + value.substring(value.lastIndexOf(File.separatorChar) + 1));
        } catch (JSchException | UnsupportedEncodingException | FileNotFoundException | SftpException e) {
            throw new ExecutorException("Error copying input file " + inputName, e);
        }
    }

}
