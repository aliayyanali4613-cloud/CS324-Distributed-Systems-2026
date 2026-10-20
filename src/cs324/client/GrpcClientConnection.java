package cs324.client;

import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Manages gRPC client connection, coordinator discovery, 
 * server-side progress streaming, and automated failover recovery.
 */
public class GrpcClientConnection {
    private GrpcClientConnection() { }

    private static JobSpec convert(JobRequest job) {
        JobSpec.Builder spec = JobSpec.newBuilder().setJobId(job.getJobId())
                .setJobType(job.getJobType().name());
        if (job.getJobType() == JobType.PRIMESUM) {
            spec.setStart(job.getBigStart().toString()).setEnd(job.getBigEnd().toString());
        } else {
            for (BigInteger n : job.getBigNumbers()) spec.addNumbers(n.toString());
        }
        return spec.build();
    }

    public static void stream(JobRequest job, Consumer<JobProgress> progress) throws Exception {
        ManagedChannel bootstrapChannel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext().build();
        BootstrapServiceGrpc.BootstrapServiceBlockingStub registry =
                BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
        JobSpec request = convert(job);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                ManagedChannel workerChannel = null;
                try {
                    // Fetch active workers from Bootstrap server
                    List<WorkerDetails> workers = registry.withDeadlineAfter(3, TimeUnit.SECONDS)
                            .getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
                    
                    if (workers == null || workers.isEmpty()) {
                        throw new IllegalStateException("No workers online in registry");
                    }

                    int coordinatorId = -1;
                    for (WorkerDetails w : workers) {
                        ManagedChannel temp = ManagedChannelBuilder
                                .forAddress(w.getHost(), w.getPort()).usePlaintext().build();
                        try {
                            coordinatorId = WorkerClusterGrpc.newBlockingStub(temp)
                                    .withDeadlineAfter(2, TimeUnit.SECONDS)
                                    .currentLeader(Empty.getDefaultInstance()).getWorkerId();
                            if (coordinatorId > 0) break;
                        } catch (Exception ignored) { 
                            // Try next node if current node is unreachable
                        } finally { 
                            temp.shutdownNow(); 
                        }
                    }

                    WorkerDetails coordinator = null;
                    for (WorkerDetails w : workers) {
                        if (w.getWorkerId() == coordinatorId) {
                            coordinator = w;
                            break;
                        }
                    }

                    if (coordinator == null) {
                        System.out.println("[Client] Waiting for coordinator election to complete...");
                        Thread.sleep(1500);
                        continue;
                    }

                    // Connect to the active coordinator and open streaming channel
                    workerChannel = ManagedChannelBuilder.forAddress(
                            coordinator.getHost(), coordinator.getPort()).usePlaintext().build();
                    
                    Iterator<JobProgress> updates = WorkerClusterGrpc.newBlockingStub(workerChannel)
                            .submitJob(request);

                    while (updates.hasNext()) {
                        JobProgress update = updates.next();
                        progress.accept(update);
                        if (update.getFinished()) return;
                    }

                    throw new IllegalStateException("Progress stream closed prematurely before completion.");

                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Client thread interrupted during recovery.");
                    }
                    System.out.println("[Client] Connection error or coordinator failover detected. Retrying in 1.5s... (" + e.getMessage() + ")");
                    Thread.sleep(1500);
                } finally {
                    if (workerChannel != null) {
                        workerChannel.shutdownNow();
                    }
                }
            }
            throw new InterruptedException("Client job stream terminated.");
        } finally {
            bootstrapChannel.shutdownNow();
        }
    }
}