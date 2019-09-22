package pt.isel.ngspipes.engine_executor.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import com.github.brunomndantas.tpl4j.pool.TaskPool;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.isel.ngspipes.engine_common.commandBuilders.ICommandBuilder;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public class MesosExecutor implements IExecutor {

    private static final String TAG = "MesosEngine";
    private static final String CHRONOS_DEPENDENCY = "scheduler/dependency";
    private static final String CHRONOS_ISO = "scheduler/iso8601";
    private static final String CHRONOS_JOB = "scheduler/jobs";
    private static final String CHRONOS_JOB_SEARCH = "scheduler/jobs/search?name=";
    private static final String EPSILON = "P1Y12M12D";
    private static final String SCHEDULE = "R1//P1Y";
    private static final String FILE_SEPARATOR = "/";

    private ChannelSftp channelSftp;
    private final Map<String, Collection<Job>> TASKS_BY_EXEC_ID = new HashMap<>();
    private final Map<String, Pipeline> pipelines = new HashMap<>();
    private final String workingDirectory;
    private final MesosInfo mesosInfo;
    private final Logger logger = LogManager.getLogger(MesosExecutor.class.getName());
    private final IExecutionProgressReporter reporter;

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
        pipelines.put(pipeline.getName(), pipeline);
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
                    ExecutorException ex = new ExecutorException("Error copying inputs.", e);
                    ExecutionState state = new ExecutionState(StateEnum.FAILED, ex);
                    pipeline.setState(state);
                }
            }
        }

        if (pipeline.getState().getState().equals(StateEnum.FAILED))
            throw (ExecutorException)pipeline.getState().getException();

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
            scheduleFinishPipeline(pipeline);
        } catch (ExecutorException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            pipeline.setState(state);
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

    private void scheduleFinishPipeline(Pipeline pipeline) {
        TaskFactory.createAndStart("finish" + pipeline.getName(), () -> {
            try {
                do {
                    boolean allSuccess = true;
                    for (Job job : pipeline.getJobs()) {
                        if (!allSuccess)
                            break;
                        if (job.getState().getState().equals(StateEnum.FAILED))
                            throw job.getState().getException();
                        allSuccess = job.getState().getState().equals(StateEnum.SUCCESS);
                    }
                    if (allSuccess)
                        break;
                    Thread.sleep(5000);
                } while(true);
                pipeline.getState().setState(StateEnum.SUCCESS);
                reporter.reportInfo("Pipeline Finished");
            } catch (Exception e) {
                pipeline.setState(new ExecutionState(StateEnum.FAILED, e));
                reporter.reportInfo("Pipeline Finished With Error");
                logger.error("Error finishing pipeline", e);
                throw e;
            }
        });
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
            if (!job.getParents().isEmpty() || (!job.getChainsFrom().isEmpty() && job.getChainsFrom().stream().allMatch((j) -> j.getState().getState().equals(StateEnum.SUCCESS))))
                url = mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY;
            HttpUtils.post(url, chronosJob);
            job.getState().setState(StateEnum.SCHEDULE);
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
                                .append(outputDir)
                                .append(FILE_SEPARATOR)
                                .append(out);
                }
            }
        }
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

    private void runInconclusiveDependencies(SimpleJob job, Pipeline pipeline, Consumer<String[]> wait, String id, String pipelineName)  {
        TaskPool.createAndStartTask("inconclusive" + pipeline.getName(), () -> {
            wait.accept(new String[]{"validateOutputs" + id, pipelineName});
            job.getState().setState(StateEnum.SUCCESS);
            List<Job> readyChildJobs = getReadyChildJobs(job, pipeline);
            try {
                if (!readyChildJobs.isEmpty()) {
                    List<Job> newChildJobs = getExpandedReadyChildJobs(pipeline, readyChildJobs);

                    Collection<ExecutionNode> childGraph = TopologicSorter.parallelSort(pipeline, newChildJobs);
                    run(pipeline, childGraph);
                    job.setInconclusive(false);
                }
            } catch (EngineCommonException e) {
                job.getState().setState(StateEnum.FAILED);
                throw new ExecutorException("Error running job " + job.getId() + ".", e);
            }
        });
    }

    private List<Job> getExpandedReadyChildJobs(Pipeline pipeline, List<Job> readyChildJobs) throws EngineCommonException {
        List<Job> newChildJobs = new LinkedList<>();
        for (Job childJob : readyChildJobs) {
            if (childJob.getSpread() != null) {
                LinkedList<Job> toRemove = new LinkedList<>();
                List<Job> expandedJobs = SpreadJobExpander.getExpandedJobs(pipeline, (SimpleJob) childJob, toRemove, this::getOutputValues, FILE_SEPARATOR);
                newChildJobs.addAll(expandedJobs);
                pipeline.removeJobs(toRemove);
                pipeline.addJobs(expandedJobs);
            } else {
                newChildJobs.add(childJob);
            }
        }
        return newChildJobs;
    }

    private List<Job> getReadyChildJobs(SimpleJob job, Pipeline pipeline) {
        List<Job> readyChildJobs = new LinkedList<>();
        synchronized (pipeline) {
            for (Job childJob : job.getChainsTo()) {
                if (childJob.isInconclusive() && childJob.getState().getState().equals(StateEnum.STAGING) && areParentsExecuted(childJob.getChainsFrom())) {
                    childJob.setInconclusive(false);
                    readyChildJobs.add(childJob);
                }
            }
        }
        return readyChildJobs;
    }

    private boolean areParentsExecuted(List<Job> chainsFrom) {
        for (Job parent : chainsFrom)
            if (!parent.getState().getState().equals(StateEnum.SUCCESS))
                return false;
        return true;
    }

    private List<String> getOutputValues(String outputName, Job job) {
        try {
            if (job.getSpread() != null) {
                return getOutputValuesFromSpreadJob(outputName, job, "");
            } else {
                return getOutputValuesFromJob(outputName, job);
            }
        } catch (ExecutorException ex) {
            job.setState(new ExecutionState(StateEnum.FAILED, ex));
        }
        return new LinkedList<>();
    }

    private List<String> getOutputValuesFromJob(String chainOutput, Job originJob) throws ExecutorException {
        Output output = originJob.getOutputById(chainOutput);
        String pattern = output.getValue().toString();
        try {
            return SSHUtils.getFilesNameByPattern(channelSftp, pattern, originJob.getEnvironment().getOutputsDirectory());
        } catch (SftpException e) {
            throw new ExecutorException("Error getting values of output: " + chainOutput + " from job " + originJob.getId(), e);
        }
    }

    private List<String> getOutputValuesFromSpreadJob(String chainOutput, Job originJob, String spreadId) {
        throw new NotImplementedException();
    }

    private void waitUntilJobFinish(String[] ids) {
        String url = mesosInfo.getChronosEndPoint() + CHRONOS_JOB_SEARCH + ids[0];
        ChronosJobStatusDto chronosJobStatusDto;
        try {
            do {
                String content = HttpUtils.get(url);
                chronosJobStatusDto = ChronosJobStatusFactory.getChronosJobStatusDto(content);
                if (chronosJobStatusDto.errorCount > 0) {
                    ExecutionState state = new ExecutionState(StateEnum.FAILED, new ExecutorException("Error executing job " + ids[0]));
                    pipelines.get(ids[1]).setState(state);
                }
                Thread.sleep(5000);
            } while (chronosJobStatusDto.successCount <= 0);
        } catch (IOException | InterruptedException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            pipelines.get(ids[1]).setState(state);
        }
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

        for (Input inputCtx : job.getInputs()) {
            if (inputCtx.getOriginStep().stream().noneMatch((stepId) -> stepId.equals(job.getId()))) {
                List<Job> chainSteps = inputCtx.getOriginJob().stream().map((j) -> {
                    if(!j.getId().equals(job.getId()))
                        return j;
                    else
                        return null;
                }).collect(Collectors.toList());
                Job chainStep = chainSteps.get(0);
                String outDir = chainStep.getEnvironment().getOutputsDirectory();

                if (chainSteps.size() > 1) {
                    addJoinChainInputsCommands(pipeline, destDir, command, parents, inputCtx, chainSteps, outDir);
                } else {
                    parents.add("validateOutputs" + pipeline.getName() + "_" + chainStep.getId());
                    Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                    List<String> usedBy = outCtx.getUsedBy();
                    if (usedBy != null && !chainStep.isInconclusive()) {
                        for (String dependent : usedBy) {
                            Output outputCtx = chainStep.getOutputById(dependent);
                            String value = outputCtx.getValue().toString();
                            command.append(getCopyInputCommand(destDir, outDir + FILE_SEPARATOR, value, false)).append(" && ");
                        }
                    } else {
                        String value = inputCtx.getValue();
                        command.append(getCopyInputCommand(destDir, outDir + FILE_SEPARATOR, value, false)).append(" && ");
                    }
                }
            }
        }
        if (command.length() > 0) {
            String changePermissions = "sudo chmod -R 777 " + destDir;
            command.append(changePermissions);
            if (job.getParents().isEmpty())
                parents = null;
            getCopyChainInputsChronosJob(jobName, command.toString(), parents);
        }
    }

    private void addJoinChainInputsCommands(Pipeline pipeline, String destDir, StringBuilder command, List<String> parents, Input inputCtx, List<Job> chainSteps, String outDir) {
        outDir = outDir.substring(0, outDir.lastIndexOf(FILE_SEPARATOR));
        String value = inputCtx.getValue().replace("[", "").replace("]", "");
        String[] values = value.split(",");
        outDir = outDir + FILE_SEPARATOR;
        for (String val : values) {
            String currOutDir = outDir;
            int separatorIndex = val.indexOf(FILE_SEPARATOR) + 1;
            if (separatorIndex > 0) {
                String subDir = val.substring(0, separatorIndex);
                if (!currOutDir.contains(subDir))
                    currOutDir = currOutDir + subDir;
            }
            command.append(getCopyInputCommand(destDir, currOutDir, val, true)).append(" && ");
        }
        chainSteps.forEach((chain) -> parents.add("validateOutputs" + pipeline.getName() + "_" + chain.getId()));
    }

    private String getChronosJob(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        String executeCmd = getExecutionCommand(job, pipeline) + "; ls";
        return getChronosJob(executeCmd, job, pipeline.getName());
    }

    private String  getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            ICommandBuilder local = CommandBuilderSupplier.getCommandBuilder("Local");
            return local.build(pipeline, job, FILE_SEPARATOR, job.getExecutionContext().getConfig());
        } catch (CommandBuilderException e) {
            logger.error(TAG + ":: Error when building step - " + job.getId(), e);
            throw new ExecutorException("Error when building step", e);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, String executionId) throws ExecutorException {
        try {
            if (job.getParents().isEmpty() && job.getChainsFrom().isEmpty())
                return getChronosJob(executeCmd, job, (j) -> executionId + "_" + job.getId());
            else
                return getDependentChronosJob(executeCmd, job, executionId);
        } catch (IOException ex) {
            throw new ExecutorException("Error creating chronos job.", ex);
        }
    }

    private String getChronosJob(String executeCmd, SimpleJob job, Function<Job, String> getJobName) throws IOException {
        String jobDir = job.getEnvironment().getOutputsDirectory();
        String command = "cd " + jobDir + " && " + executeCmd;
        String name = getJobName.apply(job);
        if (job.getExecutionContext().getContext().equalsIgnoreCase("DOCKER")) {
            Container container = getContainerInfo(job);
            DockerChronosJob dockerChronosJob = new DockerChronosJob(SCHEDULE, EPSILON, getCpus(job), command, name, getMemory(job), container);
            return JacksonUtils.serialize(dockerChronosJob);
        } else  {
            ChronosJob chronosJob = new ChronosJob(SCHEDULE, EPSILON, getCpus(job), command, name, getMemory(job));
            return JacksonUtils.serialize(chronosJob);
        }
    }

    private String getDependentChronosJob(String executeCmd, SimpleJob job, String executionId) throws IOException {
        String jobDir = job.getEnvironment().getOutputsDirectory();
        String command = " mkdir -p " + jobDir + " && " + "cd " + jobDir + " && " + executeCmd;
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
            if (!input.getChainOutput().isEmpty() && input.getOriginStep().stream().anyMatch((p) -> p.equals(parent)))
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

    private String getCopyInputCommand(String destDir, String outDir, String value, boolean isJoin) {
        int separatorIndex = value.indexOf(FILE_SEPARATOR) + 1;
        if (separatorIndex > 0) {
            String subDir = value.substring(0, separatorIndex);
            if (isJoin) {
                value = value.substring(separatorIndex);
                if (!destDir.contains(subDir))
                    destDir = destDir + subDir;
            }
        }
        String createDestDir = "sudo mkdir -p " + destDir;
        String copyInputs = " && sudo cp -R " + outDir + value + " " + destDir;

        return createDestDir + copyInputs;
    }

    private void getCopyChainInputsChronosJob(String id, String executeCmd, List<String> parents) throws ExecutorException {
        try {
            String copyTask;
            String url;
            if (parents != null) {
                copyTask = getDependentChronosJob(executeCmd, parents, id);
                url = mesosInfo.getChronosEndPoint() + CHRONOS_DEPENDENCY;
            } else{
                ChronosJob chronosJob = new ChronosJob(SCHEDULE, EPSILON, "0.5f", executeCmd, id, "2048");
                copyTask = JacksonUtils.serialize(chronosJob);
                url = mesosInfo.getChronosEndPoint() + CHRONOS_ISO;
            }
            HttpUtils.post(url, copyTask);
        } catch (IOException e) {
            logger.error("Error copying inputs to: " + id, e);
            throw new ExecutorException("Error copying inputs to: " + id, e);
        }
    }

    private String getDependentChronosJob(String executeCmd, List<String> parents, String name) throws IOException {
        String shell = "true";
        ChronosJob chronosJob = new ChronosJob(parents, EPSILON, "0.5f", shell, executeCmd, name, "512");
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
