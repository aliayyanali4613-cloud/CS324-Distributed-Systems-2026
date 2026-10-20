package cs324.common;

import cs324.worker.ElectionCandidateProto;

/**
 * Domain model representing a candidate worker during leader election.
 * Implements Comparable for deterministic sorting based on Job Absorption Capacity (JAC) and worker ID.
 */
public final class ElectionCandidate implements Comparable<ElectionCandidate> {

    private final int workerId;
    private final int jac;

    public ElectionCandidate(int workerId, int jac) {
        this.workerId = workerId;
        this.jac = jac;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac;
    }

    /**
     * Converts domain model to gRPC Protobuf message.
     */
    public ElectionCandidateProto toProto() {
        return ElectionCandidateProto.newBuilder()
                .setWorkerId(this.workerId)
                .setJac(this.jac)
                .build();
    }

    /**
     * Constructs domain model from gRPC Protobuf message.
     */
    public static ElectionCandidate fromProto(ElectionCandidateProto proto) {
        return new ElectionCandidate(proto.getWorkerId(), proto.getJac());
    }

    @Override
    public int compareTo(ElectionCandidate other) {
        if (this.jac != other.jac) {
            return Integer.compare(other.jac, this.jac);
        }
        return Integer.compare(this.workerId, other.workerId);
    }
package cs324.common;

import cs324.worker.ElectionCandidateProto;

/**
 * Domain model representing a candidate worker during leader election.
 * Implements Comparable for deterministic sorting based on Job Absorption Capacity (JAC) and worker ID.
 */
public final class ElectionCandidate implements Comparable<ElectionCandidate> {

    private final int workerId;
    private final int jac;

    public ElectionCandidate(int workerId, int jac) {
        this.workerId = workerId;
        this.jac = jac;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac;
    }

    /**
     * Converts domain model to gRPC Protobuf message.
     */
    public ElectionCandidateProto toProto() {
        return ElectionCandidateProto.newBuilder()
                .setWorkerId(this.workerId)
                .setJac(this.jac)
                .build();
    }

    /**
     * Constructs domain model from gRPC Protobuf message.
     */
    public static ElectionCandidate fromProto(ElectionCandidateProto proto) {
        if (proto == null) return null;
        return new ElectionCandidate(proto.getWorkerId(), proto.getJac());
    }

    @Override
    public int compareTo(ElectionCandidate other) {
        if (other == null) return 1;
        if (this.jac != other.jac) {
            return Integer.compare(other.jac, this.jac);
        }
        return Integer.compare(this.workerId, other.workerId);
    }

    @Override
    public String toString() {
        return "Worker " + workerId + " [JAC=" + jac + "]";
    }
}
    @Override
    public String toString() {
        return "Worker " + workerId + " [JAC=" + jac + "]";
    }
}