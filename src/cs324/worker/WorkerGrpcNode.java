package cs324.worker;

import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.*;
import cs324.job.JobCalculator;
import cs324.job.JobDistributor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** A2 worker: one independent Java process with a gRPC server. */
public class WorkerGrpcNode extends WorkerClusterGrpc.WorkerClusterImplBase {
    private static final int BOOTSTRAP_PORT = 50051;
    private static final int JOBS_PER_TERM = 5;

    private final int workerId;
    private final int port;
    private final String host;
    private final WorkerDetails identity;
    private final ManagedChannel bootstrapChannel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, WorkerDetails> neighbours = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobState> jobs = new ConcurrentHashMap<>();
    private final Set<String> runningJobs = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService background = Executors.newScheduledThreadPool(2);
    private final ExecutorService jobPool = Executors.newCachedThreadPool();
    private final AtomicInteger jac = new AtomicInteger();
    private final AtomicInteger termJobs = new AtomicInteger();
    private volatile int coordinatorId = -1;
    private volatile boolean active = true;

    private static class JobState {
        final JobPlan plan;
        final ConcurrentHashMap<Integer, TaskOutcome> completed = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, Integer> assignments = new ConcurrentHashMap<>();
        final CopyOnWriteArrayList<StreamObserver<JobProgress>> listeners = new CopyOnWriteArrayList<>();
        JobState(JobPlan plan) { this.plan = plan; }
        boolean done() { return completed.size() == plan.getTasksCount(); }
    }

    private WorkerGrpcNode(int workerId, int port) {
        this.workerId = workerId;
        this.port = port;
        this.host = "localhost";
        this.identity = WorkerDetails.newBuilder().setWorkerId(workerId).setHost(host).setPort(port).build();
        this.bootstrapChannel = ManagedChannelBuilder.forAddress("localhost", BOOTSTRAP_PORT)
                .usePlaintext().build();
        this.bootstrap = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel);
    }

    private WorkerClusterGrpc.WorkerClusterBlockingStub peer(WorkerDetails target) {
        String key = target.getHost() + ":" + target.getPort();
        ManagedChannel channel = channels.computeIfAbsent(key, k ->
                ManagedChannelBuilder.forAddress(target.getHost(), target.getPort())
                        .usePlaintext().build());
        return WorkerClusterGrpc.newBlockingStub(channel);
    }

    private List<WorkerDetails> activeWorkers() {
        return bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS)
                .getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
    }

    private void joinNetwork() {
        RegisterResponse registration = bootstrap.withDeadlineAfter(5, TimeUnit.SECONDS)
                .registerWorker(RegisterRequest.newBuilder().setWorker(identity).build());
        if (registration.hasNeighbour()) {
            WorkerDetails first = registration.getNeighbour();
            peer(first).withDeadlineAfter(3, TimeUnit.SECONDS).introduce(identity);
            neighbours.put(first.getWorkerId(), first);
            copyArchives(first);
        }
        bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS)
                .workerReady(ReadyRequest.newBuilder().setWorkerId(workerId).build());
        System.out.println("Worker " + workerId + " registered on port " + port);
    }

    private void copyArchives(WorkerDetails from) {
        try {
            ArchiveList archive = peer(from).withDeadlineAfter(4, TimeUnit.SECONDS)
                    .getArchives(Empty.getDefaultInstance());
            for (JobArchive job : archive.getJobsList()) {
                JobState state = jobs.computeIfAbsent(job.getPlan().getJob().getJobId(),
                        key -> new JobState(job.getPlan()));
                for (TaskOutcome result : job.getCompletedList()) {
                    state.completed.putIfAbsent(result.getIndex(), result);
                    state.assignments.put(result.getIndex(), result.getWorkerId());
                }
            }
        } catch (Exception e) {
            System.err.println("Could not sync previous jobs from Worker " + from.getWorkerId());
        }
    }

    private void tick() {
        if (!active) return;
        try {
            bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS).heartbeat(
                    HeartbeatRequest.newBuilder().setWorkerId(workerId).build());
            boolean started = bootstrap.withDeadlineAfter(3, TimeUnit.SECONDS)
                    .isElectionReady(Empty.getDefaultInstance()).getReady();
            if (!started) return;
            List<WorkerDetails> members = activeWorkers();
            Set<Integer> activeIds = new HashSet<>();
            for (WorkerDetails w : members) activeIds.add(w.getWorkerId());
            // A coordinator is replaced if it vanishes from Bootstrap's heartbeat registry.
            if (coordinatorId < 0 || !activeIds.contains(coordinatorId)) {
                electCoordinator(members);
            }
        } catch (Exception e) {
            System.err.println("Worker " + workerId + " cannot reach Bootstrap: " + e.getMessage());
        }
    }

    private synchronized void electCoordinator(List<WorkerDetails> members) {
        CandidateInfo winner = null;
        for (WorkerDetails w : members) {
            try {
                CandidateInfo candidate = w.getWorkerId() == workerId
                        ? CandidateInfo.newBuilder().setWorkerId(workerId).setJac(jac.get()).build()
                        : peer(w).withDeadlineAfter(2, TimeUnit.SECONDS).getCandidate(Empty.getDefaultInstance());
                if (winner == null || candidate.getJac() < winner.getJac()
                        || (candidate.getJac() == winner.getJac()
                        && candidate.getWorkerId() > winner.getWorkerId())) {
                    winner = candidate;
                }
            } catch (Exception e) {
                System.err.println("Worker " + w.getWorkerId() + " unavailable during election");
            }
        }
        if (winner == null) return;
        int previous = coordinatorId;
        coordinatorId = winner.getWorkerId();
        if (coordinatorId != previous) {
            System.out.println("ELECTED COORDINATOR: Worker " + coordinatorId);
            if (coordinatorId == workerId) {
                termJobs.set(0);
                background.execute(this::recoverJobs);
            }
        }
    }

    private void recoverJobs() {
        if (coordinatorId != workerId) return;
        try {
            for (WorkerDetails w : activeWorkers()) {
                if (w.getWorkerId() != workerId) copyArchives(w);
            }
        } catch (Exception ignored) { }
        for (JobState state : jobs.values()) {
            if (!state.done()) scheduleJob(state);
        }
    }

    private void rotateCoordinator() {
        System.out.println("Coordinator Worker " + workerId + " completed five jobs. Starting another election.");
        coordinatorId = -1;
        // Release any still-open job streams so clients can reconnect to the next leader.
        // Completed jobs were already closed by notifyOne().
        for (JobState state : jobs.values()) {
            if (!state.done()) {
                for (StreamObserver<JobProgress> listener : state.listeners) {
                    synchronized (listener) {
                        try {
                            listener.onError(Status.UNAVAILABLE
                                    .withDescription("Coordinator rotation; reconnect to new leader")
                                    .asRuntimeException());
                        } catch (Exception ignored) { }
                    }
                    state.listeners.remove(listener);
                }
            }
        }
        try {
            for (WorkerDetails w : activeWorkers()) {
                if (w.getWorkerId() == workerId) continue;
                try {
                    peer(w).withDeadlineAfter(3, TimeUnit.SECONDS)
                            .requestElection(Empty.getDefaultInstance());
                } catch (Exception ignored) { }
            }
        } catch (Exception ignored) { }
        try { electCoordinator(activeWorkers()); }
        catch (Exception ignored) { }
    }

    @Override
    public void introduce(WorkerDetails request, StreamObserver<Empty> response) {
        if (request.getWorkerId() != workerId) neighbours.put(request.getWorkerId(), request);
        respond(response, Empty.getDefaultInstance());
    }

    @Override
    public void getCandidate(Empty request, StreamObserver<CandidateInfo> response) {
        respond(response, CandidateInfo.newBuilder()
                .setWorkerId(workerId).setJac(jac.get()).build());
    }

    @Override
    public void requestElection(Empty request, StreamObserver<Empty> response) {
        coordinatorId = -1;
        respond(response, Empty.getDefaultInstance());
    }

    @Override
    public void currentLeader(Empty request, StreamObserver<LeaderInfo> response) {
        respond(response, LeaderInfo.newBuilder().setWorkerId(coordinatorId).build());
    }

    @Override
    public void runTask(TaskPart request, StreamObserver<TaskOutcome> response) {
        try {
            BigInteger value = JobCalculator.calculate(parse(request.getCalculation()));
            respond(response, TaskOutcome.newBuilder().setJobId(request.getJobId())
                    .setIndex(request.getIndex()).setWorkerId(workerId)
                    .setResult(value.toString()).build());
        } catch (Exception e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void sharePlan(JobPlan request, StreamObserver<Empty> response) {
        jobs.putIfAbsent(request.getJob().getJobId(), new JobState(request));
        respond(response, Empty.getDefaultInstance());
    }

    @Override
    public void shareResult(TaskOutcome request, StreamObserver<Empty> response) {
        JobState state = jobs.get(request.getJobId());
        if (state != null) {
            state.completed.putIfAbsent(request.getIndex(), request);
            state.assignments.put(request.getIndex(), request.getWorkerId());
            notifyListeners(state);
        }
        respond(response, Empty.getDefaultInstance());
    }

    @Override
    public void getArchives(Empty request, StreamObserver<ArchiveList> response) {
        ArchiveList.Builder archive = ArchiveList.newBuilder();
        for (JobState state : jobs.values()) {
            archive.addJobs(JobArchive.newBuilder().setPlan(state.plan)
                    .addAllCompleted(state.completed.values()).build());
        }
        respond(response, archive.build());
    }

    @Override
    public void submitJob(JobSpec request, StreamObserver<JobProgress> response) {
        if (coordinatorId != workerId) {
            response.onError(Status.FAILED_PRECONDITION
                    .withDescription("Not the coordinator; retry on elected worker")
                    .asRuntimeException());
            return;
        }
        if (request.getJobId().isBlank()) {
            response.onError(Status.INVALID_ARGUMENT.withDescription("Job ID required").asRuntimeException());
            return;
        }
        JobState state;
        try {
            state = jobs.get(request.getJobId());
            if (state == null) {
                JobPlan plan = makePlan(request);
                JobState fresh = new JobState(plan);
                JobState existing = jobs.putIfAbsent(request.getJobId(), fresh);
                state = existing == null ? fresh : existing;
                if (existing == null) replicatePlan(plan);
            }
        } catch (Exception e) {
            response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        state.listeners.add(response);
        notifyOne(state, response);
        if (!state.done()) scheduleJob(state);
    }

    private JobPlan makePlan(JobSpec spec) {
        List<WorkerDetails> members = activeWorkers();
        if (members.isEmpty()) throw new IllegalStateException("No workers online");
        JobRequest request = parse(spec);
        List<JobRequest> pieces = JobDistributor.splitJob(request, members.size() * 8);
        JobPlan.Builder plan = JobPlan.newBuilder().setJob(spec);
        int index = 0;
        for (JobRequest piece : pieces) {
            JobSpec.Builder small = JobSpec.newBuilder().setJobId(spec.getJobId())
                    .setJobType(spec.getJobType());
            if (piece.getJobType() == JobType.PRIMESUM) {
                small.setStart(piece.getBigStart().toString()).setEnd(piece.getBigEnd().toString());
            } else {
                for (BigInteger number : piece.getBigNumbers()) small.addNumbers(number.toString());
            }
            plan.addTasks(TaskPart.newBuilder().setJobId(spec.getJobId())
                    .setIndex(index++).setCalculation(small.build()).build());
        }
        return plan.build();
    }

    private static JobRequest parse(JobSpec spec) {
        JobType type = JobType.valueOf(spec.getJobType());
        if (type == JobType.PRIMESUM) {
            return new JobRequest(type, new BigInteger(spec.getStart()), new BigInteger(spec.getEnd()));
        }
        List<BigInteger> numbers = new ArrayList<>();
        for (String s : spec.getNumbersList()) numbers.add(new BigInteger(s));
        return new JobRequest(type, numbers);
    }

    private void replicatePlan(JobPlan plan) {
        try {
            for (WorkerDetails w : activeWorkers()) {
                if (w.getWorkerId() == workerId) continue;
                try { peer(w).withDeadlineAfter(5, TimeUnit.SECONDS).sharePlan(plan); }
                catch (Exception e) { System.err.println("Cannot replicate job to Worker " + w.getWorkerId()); }
            }
        } catch (Exception e) { System.err.println("Job replication delayed: " + e.getMessage()); }
    }

    private void replicateResult(TaskOutcome result) {
        try {
            for (WorkerDetails w : activeWorkers()) {
                if (w.getWorkerId() == workerId) continue;
                try { peer(w).withDeadlineAfter(3, TimeUnit.SECONDS).shareResult(result); }
                catch (Exception ignored) { }
            }
        } catch (Exception ignored) { }
    }

    private void scheduleJob(JobState state) {
        String id = state.plan.getJob().getJobId();
        if (!runningJobs.add(id)) return;
        jobPool.execute(() -> {
            try {
                List<CompletableFuture<Void>> tasks = new ArrayList<>();
                for (TaskPart part : state.plan.getTasksList()) {
                    if (state.completed.containsKey(part.getIndex())) continue;
                    tasks.add(CompletableFuture.runAsync(
                            () -> dispatchWithRetry(state, part), jobPool));
                }
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
                if (state.done() && coordinatorId == workerId) {
                    jac.incrementAndGet();
                    int served = termJobs.incrementAndGet();
                    notifyListeners(state);
                    if (served >= JOBS_PER_TERM) rotateCoordinator();
                }
            } finally {
                runningJobs.remove(id);
            }
        });
    }

    private void dispatchWithRetry(JobState state, TaskPart part) {
        Set<Integer> attempted = new HashSet<>();
        while (!state.completed.containsKey(part.getIndex()) && coordinatorId == workerId && active) {
            try {
                List<WorkerDetails> members = activeWorkers();
                List<WorkerDetails> available = new ArrayList<>();
                for (WorkerDetails w : members) if (!attempted.contains(w.getWorkerId())) available.add(w);
                if (available.isEmpty()) {
                    attempted.clear();
                    Thread.sleep(700);
                    continue;
                }
                // Distribute tasks across independent worker processes.
                WorkerDetails target = available.get(Math.floorMod(part.getIndex() + attempted.size(), available.size()));
                attempted.add(target.getWorkerId());
                state.assignments.put(part.getIndex(), target.getWorkerId());
                TaskOutcome outcome = target.getWorkerId() == workerId
                        ? calculateLocal(part)
                        : peer(target).withDeadlineAfter(40, TimeUnit.SECONDS).runTask(part);
                if (state.completed.putIfAbsent(part.getIndex(), outcome) == null) {
                    replicateResult(outcome);
                    notifyListeners(state);
                }
            } catch (Exception e) {
                // Retry unfinished work on a different worker, never discard it.
                System.err.println("Reassigning task " + part.getIndex() + " after worker error: " + e.getMessage());
              try { Thread.sleep(350); } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            Thread.currentThread().getThreadGroup().uncaughtExceptionHandler();
                            return;
                        }
        }
    }

    private TaskOutcome calculateLocal(TaskPart part) {
        BigInteger value = JobCalculator.calculate(parse(part.getCalculation()));
        return TaskOutcome.newBuilder().setJobId(part.getJobId()).setIndex(part.getIndex())
                .setWorkerId(workerId).setResult(value.toString()).build();
    }

    private JobProgress progress(JobState state) {
        JobProgress.Builder p = JobProgress.newBuilder()
                .setJobId(state.plan.getJob().getJobId())
                .setJobType(state.plan.getJob().getJobType())
                .setCompleted(state.completed.size())
                .setTotal(state.plan.getTasksCount())
                .setPercent(state.plan.getTasksCount() == 0 ? 0
                        : (int) (100L * state.completed.size() / state.plan.getTasksCount()))
                .setCoordinatorId(workerId);
        Map<Integer, int[]> stats = new TreeMap<>();
        for (Integer id : state.assignments.values()) stats.computeIfAbsent(id, x -> new int[2])[1]++;
        for (TaskOutcome result : state.completed.values())
            stats.computeIfAbsent(result.getWorkerId(), x -> new int[2])[0]++;
        for (Map.Entry<Integer, int[]> entry : stats.entrySet()) {
            p.addWorkers(WorkerStats.newBuilder().setWorkerId(entry.getKey())
                    .setCompletedTasks(entry.getValue()[0]).setAssignedTasks(entry.getValue()[1]).build());
        }
        if (state.done()) {
            List<BigInteger> results = new ArrayList<>();
            for (int i = 0; i < state.plan.getTasksCount(); i++) {
                TaskOutcome outcome = state.completed.get(i);
                if (outcome == null) return p.build();
                results.add(new BigInteger(outcome.getResult()));
            }
            p.setFinished(true).setResult(JobDistributor.combineResults(
                    JobType.valueOf(state.plan.getJob().getJobType()), results).toString());
        }
        return p.build();
    }

    private void notifyOne(JobState state, StreamObserver<JobProgress> observer) {
        synchronized (observer) {
            try {
                if (observer instanceof ServerCallStreamObserver
                        && ((ServerCallStreamObserver<?>) observer).isCancelled()) {
                    state.listeners.remove(observer);
                    return;
                }
                JobProgress snapshot = progress(state);
                observer.onNext(snapshot);
                if (snapshot.getFinished()) {
                    observer.onCompleted();
                    state.listeners.remove(observer);
                }
            } catch (Exception e) {
                state.listeners.remove(observer);
            }
        }
    }

    private void notifyListeners(JobState state) {
        for (StreamObserver<JobProgress> listener : state.listeners) notifyOne(state, listener);
    }

    private static <T> void respond(StreamObserver<T> observer, T value) {
        observer.onNext(value);
        observer.onCompleted();
    }

    private void shutdown() {
        active = false;
        try {
            bootstrap.unregisterWorker(UnregisterRequest.newBuilder().setWorkerId(workerId).build());
        } catch (Exception ignored) { }
        background.shutdownNow();
        jobPool.shutdownNow();
        bootstrapChannel.shutdownNow();
        for (ManagedChannel channel : channels.values()) channel.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WorkerGrpcNode <workerId> <port>; example: 1 2001");
            return;
        }
        int id, port;
        try { id = Integer.parseInt(args[0]); port = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.err.println("Worker ID and port must be integers."); return; }
        if (id < 1 || id > 10 || port < 1024 || port > 65535) {
            System.err.println("Worker ID must be 1-10 and port must be 1024-65535.");
            return;
        }
        WorkerGrpcNode worker = new WorkerGrpcNode(id, port);
        Server server = ServerBuilder.forPort(port).addService(worker).build().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.shutdown(); server.shutdown();
        }));
        try {
            worker.joinNetwork();
            worker.background.scheduleAtFixedRate(worker::tick, 0, 1, TimeUnit.SECONDS);
            System.out.println("gRPC Worker " + id + " running on port " + port);
            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Worker " + id + " failed to join: " + e.getMessage());
            worker.shutdown(); server.shutdownNow();
        }
    }
}
