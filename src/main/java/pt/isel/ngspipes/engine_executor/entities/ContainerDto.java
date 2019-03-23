package pt.isel.ngspipes.engine_executor.entities;

import java.util.List;

public class ContainerDto {

    public final String type;
    public final String image;
    public final List<VolumeDto> volumes;

    public ContainerDto(String type, String image, List<VolumeDto> volumes) {
        this.type = type;
        this.image = image;
        this.volumes = volumes;
    }

    public static class VolumeDto{
        public final String containerPath;
        public final String hostPath;
        public final String mode;

        public VolumeDto(String containerPath, String hostPath, String mode) {
            this.containerPath = containerPath;
            this.hostPath = hostPath;
            this.mode = mode;
        }
    }
}
