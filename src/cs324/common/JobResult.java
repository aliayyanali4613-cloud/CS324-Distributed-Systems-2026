package cs324.common;

import cs324.grpc.JobResultProto;

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

    /**
     * Converts domain model to gRPC Protobuf message.
     */
    public JobResultProto toProto() {
        return JobResultProto.newBuilder()
                .setJobId(this.jobId)
                .setResult(this.result.toString())
                .setCoordinatorId(this.coordinatorId)
                .build();
    }

    /**
     * Constructs domain model from gRPC Protobuf message.
     */
    public static JobResult fromProto(JobResultProto proto) {
        if (proto == null) return null;
        BigInteger val = proto.getResult().isEmpty() ? BigInteger.ZERO : new BigInteger(proto.getResult());
        return new JobResult(proto.getJobId(), val, proto.getCoordinatorId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobResult jobResult = (JobResult) o;
        return coordinatorId == jobResult.coordinatorId &&
                jobId.equals(jobResult.jobId) &&
                result.equals(jobResult.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, result, coordinatorId);
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