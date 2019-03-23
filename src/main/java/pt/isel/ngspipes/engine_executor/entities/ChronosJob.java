package pt.isel.ngspipes.engine_executor.entities;

import java.util.List;

public class ChronosJob {

    String schedule;
    List<String> parents;
    String epsilon;
    String cpus;
    String shell;
    String command;
    String name;
    String mem;

    public ChronosJob(String schedule, String epsilon, String cpus, String shell, String command, String name, String mem) {
        this.schedule = schedule;
        this.epsilon = epsilon;
        this.cpus = cpus;
        this.shell = shell;
        this.command = command;
        this.name = name;
        this.mem = mem;
    }

    public ChronosJob(List<String> parents, String epsilon, String cpus, String shell, String command, String name, String mem) {
        this.parents = parents;
        this.epsilon = epsilon;
        this.cpus = cpus;
        this.shell = shell;
        this.command = command;
        this.name = name;
        this.mem = mem;
    }

    public ChronosJob(String schedule, String epsilon, String cpus, String command, String name, String mem) {
        this(schedule, epsilon, cpus, "true", command, name, mem);
    }

    public ChronosJob(List<String> parents, String epsilon, String cpus, String command, String name, String mem) {
        this(parents, epsilon, cpus, "true", command, name, mem);
    }

    public ChronosJob() {
        this.shell = "true";
    }

    public String getSchedule() { return schedule; }
    public void setSchedule(String schedule) { this.schedule = schedule; }

    public List<String> getParents() { return parents; }
    public void setParents(List<String> parents) { this.parents = parents; }

    public String getEpsilon() { return epsilon; }
    public void setEpsilon(String epsilon) { this.epsilon = epsilon; }

    public String getCpus() { return cpus; }
    public void setCpus(String cpus) { this.cpus = cpus; }

    public String getShell() { return shell; }
    public void setShell(String shell) { this.shell = shell; }

    public String getCommand() { return command; }
    public void setCommand(String command) { this.command = command; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getMem() { return mem; }
    public void setMem(String mem) { this.mem = mem; }

}
