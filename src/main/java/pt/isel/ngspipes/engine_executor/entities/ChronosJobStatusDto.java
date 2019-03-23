package pt.isel.ngspipes.engine_executor.entities;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO of a Chronos job status.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChronosJobStatusDto {

    public String name;
    public int successCount;
    public int errorCount;
    public int retries;

    public String getName() { return name; }
    public int getSuccessCount() { return successCount; }
    public int getErrorCount() { return errorCount; }
    public int getRetries() { return retries; }

    public void setName(String name) { this.name = name; }
    public void getSuccessCount(int successCount) { this.successCount = successCount; }
    public void getErrorCount(int errorCount) { this.errorCount = errorCount; }
    public void getRetries(int retries) { this.retries = retries; }

    @JsonCreator
    public ChronosJobStatusDto(@JsonProperty("name") String name,
                               @JsonProperty("successCount") int successCount,
                               @JsonProperty("errorCount") int errorCount,
                               @JsonProperty("retries") int retries)
    {
        this.name = name;
        this.successCount = successCount;
        this.errorCount = errorCount;
        this.retries = retries;
    }
}
