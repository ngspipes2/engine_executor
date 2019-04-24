package pt.isel.ngspipes.engine_executor.implementations;

import org.apache.logging.log4j.LogManager;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Output;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;
import pt.isel.ngspipes.engine_common.executionReporter.IExecutionProgressReporter;
import pt.isel.ngspipes.engine_common.utils.CommandBuilderSupplier;
import pt.isel.ngspipes.engine_common.utils.IOUtils;
import pt.isel.ngspipes.engine_executor.utils.ProcessRunner;

import java.io.File;
import java.io.IOException;


public class LinuxExecutor extends LocalExecutor {

    private static final String TAG = "VagrantExecutor";
    private static final String FILE_SEPARATOR = "/";
    private static final String WORK_DIRECTORY = System.getProperty("user.home") + File.separatorChar + "NGSPipes" +
            File.separatorChar + "Engine";

    public LinuxExecutor(IExecutionProgressReporter reporter, String workingDirectory) {
        super(LogManager.getLogger(LinuxExecutor.class.getName()), TAG, FILE_SEPARATOR, reporter, workingDirectory);
    }

    public LinuxExecutor(IExecutionProgressReporter reporter) {
       this(reporter, WORK_DIRECTORY);
    }

    @Override
    void finishSuccessfully(Pipeline pipeline) {
        pipeline.getState().setState(StateEnum.SUCCESS);
    }

    @Override
    void finishOnError(Pipeline pipeline, Exception ex) {
        ExecutionState failed = new ExecutionState(StateEnum.FAILED, ex);
        pipeline.setState(failed);
    }

    @Override
    String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            return CommandBuilderSupplier.getCommandBuilder(job.getExecutionContext().getContext()).build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
        } catch (CommandBuilderException e) {
            logger.error(tag + ":: Error when building step - " + job.getId(), e);
            throw new ExecutorException("Error when building step", e);
        }
    }

    @Override
    void executeJob(String executeCmd, Job job, Pipeline pipeline) throws ExecutorException {
        try {
            ProcessRunner.run(executeCmd, job.getEnvironment().getWorkDirectory(), reporter);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error executing job: " + job.getId(), e);
        }
    }

    @Override
    public void execute(Pipeline pipeline) throws ExecutorException {
        copyPipelineInputs(pipeline);
        try {
            ProcessRunner.run("sudo chmod -R 777 " + pipeline.getEnvironment().getWorkDirectory(), pipeline.getEnvironment().getWorkDirectory(), reporter);
        } catch (EngineCommonException e) {
            throw new ExecutorException("Error changing permissions: " + pipeline.getName(), e);
        }
        run(pipeline, pipeline.getGraph());
    }

}
