package pt.isel.ngspipes.engine_executor.entities;

import java.util.List;

public class DockerChronosJob extends ChronosJob {

    Container container;

    public DockerChronosJob(String schedule, String epsilon, String cpus, String shell, String command, String name, String mem, Container container) {
        super(schedule, epsilon, cpus, shell, command, name, mem);
        this.container = container;
    }

    public DockerChronosJob(List<String> parents, String epsilon, String cpus, String shell, String command, String name, String mem, Container container) {
        super(parents, epsilon, cpus, shell, command, name, mem);
        this.container = container;
    }

    public DockerChronosJob(String schedule, String epsilon, String cpus, String command, String name, String mem, Container container) {
        super(schedule, epsilon, cpus, command, name, mem);
        this.container = container;
    }

    public DockerChronosJob(List<String> parents, String epsilon, String cpus, String command, String name, String mem, Container container) {
        super(parents, epsilon, cpus, command, name, mem);
        this.container = container;
    }

    public DockerChronosJob() { }

    public Container getContainer() { return container; }
    public void setContainer(Container container) { this.container = container; }

}
