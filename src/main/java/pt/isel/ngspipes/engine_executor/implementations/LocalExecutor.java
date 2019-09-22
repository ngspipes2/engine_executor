package pt.isel.ngspipes.engine_executor.implementations;

import com.github.brunomndantas.tpl4j.factory.TaskFactory;
import com.github.brunomndantas.tpl4j.task.Task;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;
import pt.isel.ngspipes.engine_common.entities.ExecutionNode;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import pt.isel.ngspipes.engine_common.interfaces.IExecutor;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_common.utils.SpreadJobExpander;
import pt.isel.ngspipes.engine_common.utils.TopologicSorter;
import pt.isel.ngspipes.engine_common.utils.ValidateUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class LocalExecutor implements IExecutor {

    private final Map<String, Map<Job, Task<Void>>> TASKS_BY_EXEC_ID = new HashMap<>();
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
    public void getPipelineOutputs(Pipeline pipeline, String outputDirectory) throws ExecutorException {
        for (Output output : pipeline.getOutputs()) {
            getPipelineOutput(output, pipeline, outputDirectory);
        }
    }

    @Override
    public String getWorkingDirectory() { return workingDirectory; }

    @Override
    public String getFileSeparator() {
        return File.separatorChar + "";
    }


    abstract void finishSuccessfully(Pipeline pipeline) throws ExecutorException, ProgressReporterException;
    abstract void finishOnError(Pipeline pipeline, Exception ex) throws ExecutorException, ProgressReporterException;
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

        Map<Job, Task<Void>> taskMap = TASKS_BY_EXEC_ID.get(pipeline.getName());
        createTasks(graph, pipeline, taskMap);
        scheduleFinishPipelineTask(pipeline, taskMap);
        scheduleChildTasks(pipeline, taskMap);
        executeParents(graph, taskMap);
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
                    updateState(job, e, StateEnum.FAILED);
                    finishOnError(pipeline, e);
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
                boolean allSuccess = true;
                do {
                    for (Map.Entry<Job, Task<Void>> entry : taskMap.entrySet()) {
                        if (!allSuccess)
                            break;
                        Job job = entry.getKey();
                        if (job.getState().getState().equals(StateEnum.FAILED))
                            throw job.getState().getException();
                        allSuccess = job.getState().getState().equals(StateEnum.SUCCESS);
                    }
                    if (allSuccess)
                        break;
                    Thread.sleep(5000);
                } while (true);
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

    private void getPipelineOutput(Output output, Pipeline pipeline, String outputDirectory) throws ExecutorException {
        Job outputJob = pipeline.getJobById(output.getOriginJob());
        try {
            String type = output.getType();
            String outVal = File.separatorChar + output.getValue().toString();
            String jobOutputsDirectory = outputJob.getEnvironment().getOutputsDirectory();
            if (type.equalsIgnoreCase("directory")) {
                IOUtils.copyDirectory(jobOutputsDirectory + outVal, outputDirectory + outVal);
            } else if (type.equalsIgnoreCase("file")) {
                IOUtils.copyFile(jobOutputsDirectory + outVal, outputDirectory + outVal);
            } else if (type.equalsIgnoreCase("file[]")) {
                outVal = outVal.replace("[", "");
                outVal = outVal.replace("]", "");
                String[] values = outVal.split(",");
                for (String value : values)
                    IOUtils.copyFile(jobOutputsDirectory + value, outputDirectory + value);
            }
        } catch (IOException e) {
            throw new ExecutorException("Error getting pipeline " + output.getName() + " output.", e);
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
            execute(pipeline, simpleJob);

            job.getState().setState(StateEnum.SUCCESS);
            List<Job> readyChildJobs = getReadyChildJobs(simpleJob, pipeline);
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

        } catch (EngineCommonException e) {
            job.getState().setState(StateEnum.FAILED);
            throw new ExecutorException("Error running task " + job.getId(), e);
        }
    }

    private List<Job> getExpandedReadyChildJobs(Pipeline pipeline, List<Job> readyChildJobs) throws EngineCommonException {
        List<Job> newChildJobs = new LinkedList<>();
        for (Job childJob : readyChildJobs) {
            if (childJob.getSpread() != null) {
                LinkedList<Job> toRemove = new LinkedList<>();
                List<Job> expandedJobs = SpreadJobExpander.getExpandedJobs(pipeline, (SimpleJob) childJob, toRemove, this::getOutputValues, fileSeparator);
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
            validateOutputs(job);
        } catch (ExecutorException | ProgressReporterException e) {
            logger.error("Executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
            updateState(job, e, StateEnum.FAILED);
            try {
                finishOnError(pipeline, e);
            } catch (ProgressReporterException e1) {
                throw new ExecutorException("Error executing step: " + job.getId()
                        + " from pipeline: " + pipeline.getName(), e);
            }
            throw new ExecutorException("Error executing step: " + job.getId()
                    + " from pipeline: " + pipeline.getName(), e);
        }
    }

    private void validateOutputs(SimpleJob job) throws ExecutorException {
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

        if (!destDir.contains(jobId)) {
            destDir = destDir + jobId + File.separatorChar;
        }

        for (Input inputCtx : job.getInputs()) {
            if (inputCtx.getOriginStep().stream().noneMatch((stepId) -> stepId.equals(jobId))) {
                List<Job> chainSteps = inputCtx.getOriginJob().stream().map((j) -> {
                    if(!j.getId().equals(jobId))
                        return j;
                    else
                        return null;
                }).collect(Collectors.toList());

                Job chainStep = chainSteps.get(0);
                String outDir = chainStep.getEnvironment().getOutputsDirectory();
                Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());

                if (chainSteps.size() > 1) {
                    addJoinChainInputsCommands(destDir, inputCtx, outDir, outCtx.getType());
                } else {
                    List<String> usedBy = outCtx.getUsedBy();
                    if (usedBy != null && !chainStep.isInconclusive()) {
                        for (String dependent : usedBy) {
                            Output outputCtx = chainStep.getOutputById(dependent);
                            String value = outputCtx.getValue().toString();
                            copyInput(destDir, outDir + File.separatorChar, value, outCtx.getType());
                        }
                    } else {
                        String value = inputCtx.getValue();
                        copyInput(destDir, outDir + File.separatorChar, value, outCtx.getType());
                    }
                }
//               for (Job chainStep : chainSteps) {
//                   String outDir = chainStep.getEnvironment().getOutputsDirectory() + fileSeparator;
//                   Output outCtx = chainStep.getOutputById(inputCtx.getChainOutput());
//                   List<String> usedBy = outCtx.getUsedBy();
//
//                   if (usedBy != null) {
//                       for (String dependent : usedBy) {
//                           Output outputCtx = chainStep.getOutputById(dependent);
//                           String value = outputCtx.getValue().toString();
//                           copyInput(destDir, outDir, value, outputCtx.getType());
//                       }
//                   } else {
//                       String value = outCtx.getValue().toString();
//                       copyInput(destDir, outDir, value, outCtx.getType());
//                   }
//               }
            }
        }
    }

    private void addJoinChainInputsCommands(String destDir, Input inputCtx, String outDir, String type) throws ExecutorException {
        outDir = outDir.substring(0, outDir.lastIndexOf(fileSeparator));
        String value = inputCtx.getValue().replace("[", "").replace("]", "");
        String[] values = value.split(",");
        outDir = outDir + fileSeparator;
        for (String val : values) {
            String currOutDir = outDir;
            int separatorIndex = val.indexOf(fileSeparator) + 1;
            if (separatorIndex > 0) {
                String subDir = val.substring(0, separatorIndex);
                if (!currOutDir.contains(subDir))
                    currOutDir = currOutDir + subDir;
            }

            copyInput(destDir, outDir, val, type);
        }
    }

    private void copyInput(String destDir, String outDir, String value, String type) throws ExecutorException {
        try {
            String source = outDir + value;
            source = source.replace(fileSeparator, File.separatorChar + "");
            destDir = destDir + value;
            destDir = destDir.replace(fileSeparator, File.separatorChar + "");
            if (type.equals("directory"))
                IOUtils.copyDirectory(source, destDir);
            else {
                IOUtils.copyFile(source, destDir);
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

    private void updateState(Job job, Exception e, StateEnum state) {
        ExecutionState newState = new ExecutionState(state, e);
        job.setState(newState);
    }

    private void copyInput(Job job, Input input) throws ExecutorException {
        String type = input.getType();
        if (type.contains("file") || type.equalsIgnoreCase("directory")) {
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
                            copyInput(val, job, fileName);
                    } else  {
                        copyInput(value, job, fileName);
                    }
                    input.setValue(fileName);
                }
            }
        }
    }

    private void copyInput(String input, Job stepCtx, String inputName) throws ExecutorException {
        String destInput = stepCtx.getEnvironment().getWorkDirectory() + File.separatorChar + inputName;
        try {
            IOUtils.copyFile(input, destInput);
        } catch (IOException e) {
            throw new ExecutorException("Error copying input file " + inputName, e);
        }
    }

}
