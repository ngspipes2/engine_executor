package pt.isel.ngspipes.engine_executor.entities;

import java.util.List;

public class ChronosJobDto {

        public final String name;
        public final String owner;
        public final int mem;
        public final int disk;
        public final float cpus;
        public final String epsilon;
        public final String command;
        public final ContainerDto container;
        public final String schedule;
        public final List<String> parents;

        public ChronosJobDto( String name, String owner, int mem, int disk, float cpus, String epsilon,
                              String command, ContainerDto container, String schedule, List<String> parents)
        {
            this.name = name;
            this.owner = owner;
            this.mem = mem;
            this.disk = disk;
            this.cpus = cpus;
            this.schedule = schedule;
            this.epsilon = epsilon;
            this.command = command;
            this.container = container;
            this.parents = parents;
        }

}
