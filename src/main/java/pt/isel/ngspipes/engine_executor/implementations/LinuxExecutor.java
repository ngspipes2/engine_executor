package pt.isel.ngspipes.engine_executor.implementations;

import pt.isel.ngspipes.engine_executor.utils.ProcessRunner;
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


public class LinuxExecutor extends LocalExecutor {

    public LinuxExecutor(String tag, String fileSeparator, IExecutionProgressReporter reporter, String workingDirectory) {
        super(LogManager.getLogger(LinuxExecutor.class.getName()), tag, fileSeparator, reporter, workingDirectory);
    }

    @Override
    void finishSuccessfully(Pipeline pipeline) throws ExecutorException, ProgressReporterException {
        pipeline.getState().setState(StateEnum.SUCCESS);
    }

    @Override
    void finishOnError(Pipeline pipeline) throws ExecutorException, ProgressReporterException {
        pipeline.getState().setState(StateEnum.FAILED);
    }

    @Override
    String getExecutionCommand(SimpleJob job, Pipeline pipeline) throws ExecutorException {
        try {
            return job.getCommandBuilder().build(pipeline, job, fileSeparator, job.getExecutionContext().getConfig());
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
        run(pipeline, pipeline.getGraph());
    }

}
