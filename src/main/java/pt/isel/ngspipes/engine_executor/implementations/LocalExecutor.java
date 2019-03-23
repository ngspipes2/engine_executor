package pt.isel.ngspipes.engine_executor.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import com.github.brunomndantas.tpl4j.task.Task;
import pt.isel.ngspipes.engine_common.entities.ExecutionNode;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import pt.isel.ngspipes.engine_common.interfaces.IExecutor;
import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_common.utils.SpreadJobExpander;
import pt.isel.ngspipes.engine_common.utils.TopologicSorter;
import pt.isel.ngspipes.engine_common.utils.ValidateUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public abstract class LocalExecutor implements IExecutor {

    private final Map<String, Map<Job, Task<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
    final Map<String, Pipeline> pipelines = new HashMap<>();
    final Logger logger;
    final String tag;
    final String fileSeparator;
    final IExecutionProgressReporter reporter;
    final String workingDirectory;

    public LocalExecutor(Logger logger, String tag, String fileSeparator, IExecutionProgressReporter reporter, String workingDirectory) {
        this.logger = logger;
        this.tag = tag;
        this.fileSeparator = fileSeparator;
        this.reporter = reporter;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public boolean stop(String executionId) throws ExecutorException {
        if (TASKS_BY_EXEC_ID.containsKey(executionId))
            TASKS_BY_EXEC_ID.get(executionId).values().forEach(Task::cancel);

        return true;
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
    public void getPipelineOutputs(String executionId, String outputDirectory) {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public String getWorkingDirectory() { return workingDirectory; }

    abstract void finishSuccessfully(Pipeline pipeline) throws ExecutorException, ProgressReporterException;
    abstract void finishOnError(Pipeline pipeline) throws ExecutorException, ProgressReporterException;
    abstract String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException;
    abstract void executeJob(String executeCmd, Job job, Pipeline pipeline) throws ExecutorException;



    protected void copyPipelineInputs(Pipeline pipeline) throws ExecutorException {
        logger.trace(tag + ":: Copying pipeline " + pipeline.getName() + " "
                + pipeline.getName() + " inputs.");

        for (Job job : pipeline.getJobs()) {
            copyInputs(job, job.getInputs());
        }
    }

    protected void run(Pipeline pipeline, Collection<ExecutionNode> graph) throws ExecutorException {
        if (!TASKS_BY_EXEC_ID.containsKey(pipeline.getName())) {
            TASKS_BY_EXEC_ID.put(pipeline.getName(), new HashMap<>());
            pipeline.getState().setState(StateEnum.RUNNING);
        }
        Map<Job, Task<Void>> taskMap = new HashMap<>();
        createTasks(graph, pipeline, taskMap);
        scheduleFinishPipelineTask(pipeline, taskMap);
        scheduleChildTasks(pipeline, taskMap);
        scheduleParentsTasks(graph, pipeline.getName(), taskMap);
    }



    private void copyInputs(Job job, List<Input> inputs) throws ExecutorException {
        for (Input input : inputs) {
            copyInput(job, input);
            copyInputs(job, input.getSubInputs());
        }
    }

    private void createTasks(Collection<ExecutionNode> executionGraph, Pipeline pipeline, Map<Job, Task<Void>> tasks) {
        for (ExecutionNode node : executionGraph) {
            Job job = node.getJob();

            if (tasks.containsKey(job))
                continue;

            Task<Void> task = TaskFactory.create(job.getId(), () -> {
                try {
                    runTask(job, pipeline);
                } catch (ExecutorException e) {
                    updateState(pipeline, job, e, StateEnum.FAILED);
                    finishOnError(pipeline);
                    throw e;
                }
            });

            tasks.put(job, task);
            TASKS_BY_EXEC_ID.get(pipeline.getName()).put(job, task);
            createTasks(node.getChilds(), pipeline, tasks);
        }
    }

    private void scheduleFinishPipelineTask(Pipeline pipeline, Map<Job, Task<Void>> taskMap) {
        Task<Void> task = TaskFactory.create("finish" + pipeline.getName(), () -> {
            try {
                pipeline.getState().setState(StateEnum.SUCCESS);
                reporter.reportInfo("Pipeline Finished");
                finishSuccessfully(pipeline);
            } catch (Exception e) {
                reporter.reportError("Error finishing pipeline");
                logger.error("Error finishing pipeline", e);
                throw e;
            }
        });
        Task<Collection<Void>> tasks = TaskFactory.whenAll("parent_finish" + pipeline.getName(), new ArrayList<>(taskMap.values()));
        tasks.then(task);
    }

    private void scheduleChildTasks(Pipeline pipeline, Map<Job, Task<Void>> tasksMap) {
        for (Map.Entry<Job, Task<Void>> entry : tasksMap.entrySet()) {
            Job job = entry.getKey();
            job.getState().setState(StateEnum.SCHEDULE);
            if (!job.getParents().isEmpty()) {
                runWhenAll(pipeline, job, tasksMap);
            }
        }
    }

    private void scheduleParentsTasks(Collection<ExecutionNode> executionGraph, String executionId,
                                      Map<Job, Task<Void>> taskMap) throws ExecutorException {
        try {
            executeParents(executionGraph, taskMap);
        } catch (ExecutorException e) {
            ExecutionState state = new ExecutionState(StateEnum.FAILED, e);
            updatePipelineState(executionId, state);
            throw e;
        }
    }

    private void executeParents(Collection<ExecutionNode> executionGraph, Map<Job, Task<Void>> task)
            throws ExecutorException {
        for (ExecutionNode parentNode : executionGraph) {
            Job job = parentNode.getJob();

            try {
                logger.trace(tag + ":: Executing step " + job.getId());
                task.get(job).start();
            } catch (Exception e) {
                logger.error(tag + ":: Executing step " + job.getId(), e);
                throw new ExecutorException("Error executing step: " + job.getId(), e);
            }
        }
    }

    private void runWhenAll(Pipeline pipeline, Job job, Map<Job, Task<Void>> taskMap) {
        Collection<Task<Void>> parentsTasks = getParentsTasks(pipeline, job.getParents());
        Task<Collection<Void>> tasks = TaskFactory.whenAll(job.getId() + "_parents", parentsTasks);
        tasks.then(taskMap.get(job));
    }

    private Collection<Task<Void>> getParentsTasks(Pipeline pipeline, Collection<String> parents) {
        Collection<Task<Void>> parentsTasks = new LinkedList<>();

        for (String parent : parents) {
            Map<Job, Task<Void>> jobBasicTaskMap = TASKS_BY_EXEC_ID.get(pipeline.getName());
            jobBasicTaskMap.keySet().forEach((job) -> {
                if (job.getId().equalsIgnoreCase(parent)) {
                    Task<Void> task = jobBasicTaskMap.get(job);
                    parentsTasks.add(task);
                }
            });
        }

        return parentsTasks;
    }

    private void runTask(Job job, Pipeline pipeline) throws ExecutorException {
        try {
            ValidateUtils.validatePipelineState(pipeline);
            ValidateUtils.validateResources(job, pipeline);
            job.getState().setState(StateEnum.STAGING);

            SimpleJob simpleJob = (SimpleJob) job;
            if (simpleJob.getSpread() != null) {
                LinkedList<ExecutionNode> graph = new LinkedList<>();
                SpreadJobExpander.expandSpreadJob(pipeline, simpleJob, graph, this::getOutputValues, fileSeparator);
                run(pipeline, graph);
            } else {
                execute(pipeline, simpleJob);
            }

            if (job.isInconclusive()) {
                job.setInconclusive(false);
                List<Job> childJobs = new LinkedList<>();
                for (Job childJob : job.getChainsTo()) {
                    if (childJob.getSpread() != null) {
                        childJobs.addAll(SpreadJobExpander.getExpandedJobs(pipeline, (SimpleJob) childJob, new LinkedList<>(), this::getOutputValues, fileSeparator));
                    } else {
                        childJobs.add(childJob);
                    }
                }
                Collection<ExecutionNode> childGraph = TopologicSorter.parallelSort(pipeline, childJobs);
                run(pipeline, childGraph);
            }

        } catch (EngineCommonException e) {
            job.getState().setState(StateEnum.FAILED);
            throw new ExecutorException("Error running task " + job.getId(), e);
        }
        job.getState().setState(StateEnum.SUCCESS);
    }

    private void execute(Pipeline pipeline, SimpleJob stepCtx) throws ExecutorException {
        copyChainInputs(stepCtx, pipeline);
        run(stepCtx, pipeline);
    }

    private void run(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        String executeCmd = getExecutionCommand(job, pipeline);
        try {
            reporter.reportInfo("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName());
            IOUtils.createFolder(job.getEnvironment().getOutputsDirectory());
            job.getState().setState(StateEnum.RUNNING);
            executeJob(executeCmd, job, pipeline);
            validateOutputs(job, pipeline.getName());
        } catch (ExecutorException | ProgressReporterException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            updateState(pipeline, job, e, StateEnum.FAILED);
            throw new ExecutorException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private void validateOutputs(SimpleJob job, String pipelineName) throws ExecutorException {
        for (Output outCtx : job.getOutputs()) {
            String type = outCtx.getType();
            if (type.contains("ile") || type.contains("irectory")) {
                String out = outCtx.getValue().toString();
                out = out.replace(fileSeparator, File.separatorChar + "");
                try {
                    IOUtils.findFiles(job.getEnvironment().getOutputsDirectory(), out);
                } catch (IOException e) {
                    throw new ExecutorException("Output " + outCtx.getName() +
                            " not found. Error running job " + job.getId(), e);
                }
            }
        }
    }

    private void copyChainInputs(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        String jobId = job.getId();
        String destDir = job.getEnvironment().getWorkDirectory() + File.separatorChar;

        for (Input inputCtx : job.getInputs()) {
            if (!inputCtx.getOriginStep().equals(jobId)) {
                Job chainStep = pipeline.getJobById(inputCtx.getOriginStep());
                String outDir = chainStep.getEnvironment().getOutputsDirectory() + fileSeparator;
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
                List<String> usedBy = outCtx.getUsedBy();

                if (usedBy != null) {
                    for (String dependent : usedBy) {
                        Output outputCtx = chainStep.getOutputById(dependent);
                        String value = outputCtx.getValue().toString();
                        copyInput(destDir, outDir, value, outputCtx.getType());
                    }
                } else {
                    String value = outCtx.getValue().toString();
                    copyInput(destDir, outDir, value, outCtx.getType());
                }
            }
        }
    }

    private void copyInput(String destDir, String outDir, String value, String type) throws ExecutorException {
        try {
            String source = outDir + value;
            source = source.replace(fileSeparator, File.separatorChar + "");
            destDir = destDir.replace(fileSeparator, File.separatorChar + "");
            if (type.equals("directory"))
                IOUtils.copyDirectory(source, destDir + value);
            else {
                if (value.contains(fileSeparator))
                    value = value.substring(value.indexOf(fileSeparator) + 1);
                IOUtils.copyFile(source, destDir + value);
            }
        } catch (IOException e) {
            throw new ExecutorException("Error copying input: " + value , e);
        }
    }

    private List<String> getOutputValues(String outputName, Job job) {
        if (job.getSpread() != null) {
            return getOutputValuesFromSpreadJob(outputName, job, "");
        } else {
            return getOutputValuesFromJob(outputName, job);
        }
    }

    private List<String> getOutputValuesFromJob(String outputName, Job originJob) {
        Output out = originJob.getOutputById(outputName);
        String outputsDirectory = originJob.getEnvironment().getOutputsDirectory();
        String pattern = out.getValue().toString();
        return new LinkedList<>(IOUtils.getFileNamesByPattern(outputsDirectory, pattern));
    }

    private List<String> getOutputValuesFromSpreadJob(String outputName, Job originJob, String spreadId) {
        Output out = originJob.getOutputById(outputName);
        String outputsDirectory = originJob.getEnvironment().getOutputsDirectory();
        String patterns = out.getValue().toString();
        String appendAtEnd = "";
        if (out.getType().contains("ile[]")) {
            appendAtEnd = patterns.substring(patterns.lastIndexOf("]") + 1);
            patterns = patterns.replace(appendAtEnd, "");
        }
        int beginIdx = spreadId.lastIndexOf(originJob.getId()) + originJob.getId().length();
        int idx = Integer.parseInt(spreadId.substring(beginIdx));
        String pattern = SpreadJobExpander.getValues(patterns).get(idx) + appendAtEnd;
        List<String> fileNamesByPattern = IOUtils.getFileNamesByPattern(outputsDirectory + fileSeparator + spreadId, pattern);
        return new LinkedList<>(fileNamesByPattern);
    }

    private void updateState(Pipeline pipeline, Job job, Exception e, StateEnum state) {
        ExecutionState newState = new ExecutionState(state, e);
        job.setState(newState);
        pipeline.setState(newState);
    }

    private void updatePipelineState(String executionId, ExecutionState state) {
        pipelines.get(executionId).setState(state);
    }

    private void copyInput(Job job, Input input) throws ExecutorException {
        String type = input.getType();
        if (type.equalsIgnoreCase("file") || type.equalsIgnoreCase("directory") || type.equalsIgnoreCase("file[]")) {
            if (input.getChainOutput() == null || input.getChainOutput().isEmpty()) {
                String value = input.getValue();
                if (type.equalsIgnoreCase("directory")) {
                    IOUtils.createFolder(job.getEnvironment().getWorkDirectory() + File.separatorChar + value);
                } else {
                    String fileName = value.substring(value.lastIndexOf(File.separatorChar) + 1);
                    if (type.contains("[]") || job.getSpread() != null) {
                        value = value.replace("[", "");
                        value = value.replace("]", "");
                        value = value.replace(" ", "");
                        String[] values = value.split(",");
                        for (String val : values)
                            copyInput(val, job);
                    } else  {
                        copyInput(value, job);
                    }
                    input.setValue(fileName);
                }
            }
        }
    }

    private void copyInput(String input, Job stepCtx) throws ExecutorException {
        String inputName = input.substring(input.lastIndexOf(File.separatorChar) - 1);
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + inputName;
        try {
            IOUtils.copyFile(input, destInput);
        } catch (IOException e) {
            throw new ExecutorException("Error copying input file " + inputName, e);
        }
    }

}
