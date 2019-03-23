package pt.isel.ngspipes.engine_executor.entities;

import java.util.List;

public class Container {

    String type;
    String image;
    String network;
    List<Volume> volumes;

    public Container(String type, String image, String network, List<Volume> volumes) {
        this.type = type;
        this.image = image;
        this.network = network;
        this.volumes = volumes;
    }

    public Container(String image, List<Volume> volumes) {
        this();
        this.image = image;
        this.volumes = volumes;
    }

    public Container() {
        this.type = "DOCKER";
        this.network = "HOST";
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getNetwork() { return network; }
    public void setNetwork(String network) { this.network = network; }

    public List<Volume> getVolumes() { return volumes; }
    public void setVolumes(List<Volume> volumes) { this.volumes = volumes; }
}
