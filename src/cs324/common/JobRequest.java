package cs324.common;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Domain model representing a distributed computation request (MAX, PRIMESUM, PRIMECOUNT).
 * Updated for robust null safety, immutable job identifiers, and gRPC compatibility.
 */
public class JobRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final JobType jobType;
    private final List<BigInteger> numbers;
    private final BigInteger start;
    private final BigInteger end;

    /**
     * Primary constructor with explicit job ID support for failover retries.
     */
    public JobRequest(String jobId, JobType jobType, List<? extends Number> numbers, BigInteger start, BigInteger end) {
        this.jobId = (jobId != null && !jobId.isBlank()) ? jobId : UUID.randomUUID().toString();
        this.jobType = Objects.requireNonNull(jobType, "JobType cannot be null");
        
        this.numbers = new ArrayList<>();
        if (numbers != null) {
            for (Number number : numbers) {
                if (number != null) {
                    this.numbers.add(new BigInteger(number.toString()));
                }
            }
        }
        
        this.start = start != null ? start : BigInteger.ZERO;
        this.end = end != null ? end : BigInteger.ZERO;
    }

    // Constructor for MAX and PRIMECOUNT (generates new unique ID)
    public JobRequest(JobType jobType, List<? extends Number> numbers) {
        this(UUID.randomUUID().toString(), jobType, numbers, BigInteger.ZERO, BigInteger.ZERO);
    }

    // Constructor for PRIMESUM (BigInteger bounds)
    public JobRequest(JobType jobType, BigInteger start, BigInteger end) {
        this(UUID.randomUUID().toString(), jobType, Collections.emptyList(), start, end);
    }

    // Constructor for PRIMESUM (int bounds) for legacy compatibility
    public JobRequest(JobType jobType, int start, int end) {
        this(UUID.randomUUID().toString(), jobType, Collections.emptyList(), BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    public String getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public List<BigInteger> getBigNumbers() {
        return Collections.unmodifiableList(numbers);
    }

    public BigInteger getBigStart() {
        return start;
    }

    public BigInteger getBigEnd() {
        return end;
    }

    // Original getters for compatibility with existing classes
    public List<Integer> getNumbers() {
        List<Integer> result = new ArrayList<>();
        for (BigInteger number : numbers) {
            result.add(number.intValueExact());
        }
        return result;
    }

    public int getStart() {
        return start.intValueExact();
    }

    public int getEnd() {
        return end.intValueExact();
    }

    @Override
    public String toString() {
        return "JobRequest{" +
                "jobId='" + jobId + '\'' +
                ", jobType=" + jobType +
                ", numbers=" + numbers +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}