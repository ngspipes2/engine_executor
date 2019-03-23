package pt.isel.ngspipes.engine_executor.entities;

public class VagrantConfig {

    private String ip_address;
    private String hostname;
    private String share_path;
    private int vm_ram;
    private int vm_cpus;
    private String pipeline_name;

    public VagrantConfig(String ip_address, String hostname, String share_path, int vm_ram, int vm_cpus, String pipeline_name) {
        this.ip_address = ip_address;
        this.hostname = hostname;
        this.vm_ram = vm_ram;
        this.vm_cpus = vm_cpus;
        this.share_path = share_path;
        this.pipeline_name = "/" + pipeline_name;
    }

    public VagrantConfig() {}

    public String getIp_address() { return ip_address; }
    public void setIp_address(String ip_address) { this.ip_address = ip_address; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public String getShare_path() { return share_path; }
    public void setShare_path(String share_path) { this.share_path = share_path; }

    public int getVm_ram() { return vm_ram; }
    public void setVm_ram(int vm_ram) { this.vm_ram = vm_ram; }

    public int getVm_cpus() { return vm_cpus; }
    public void setVm_cpus(int vm_cpus) { this.vm_cpus = vm_cpus; }

    public String getPipeline_name() { return pipeline_name; }
    public void setPipeline_name(String pipeline_name) { this.pipeline_name = "/" + pipeline_name; }
}
