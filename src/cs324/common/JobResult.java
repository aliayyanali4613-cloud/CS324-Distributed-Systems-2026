package cs324.common;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Immutable domain model representing the finalized computation result of a job.
 * Enhanced with validation and Protobuf serialization support.
 */
public class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final BigInteger result;
    private final int coordinatorId;

    public JobResult(String jobId, BigInteger result, int coordinatorId) {
        this.jobId = Objects.requireNonNull(jobId, "JobId cannot be null");
        this.result = result != null ? result : BigInteger.ZERO;
        this.coordinatorId = coordinatorId;
    }

    public JobResult(String jobId, long result, int coordinatorId) {
        this(jobId, BigInteger.valueOf(result), coordinatorId);
    }

    public String getJobId() {
        return jobId;
    }

    public BigInteger getResult() {
        return result;
    }

    public int getCoordinatorId() {
        return coordinatorId;
    }

    @Override
    public String toString() {
        return "JobResult{" +
                "jobId='" + jobId + '\'' +
                ", result=" + result +
                ", coordinatorId=" + coordinatorId +
                '}';
    }
}