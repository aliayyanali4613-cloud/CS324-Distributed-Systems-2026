package cs324.bootstrap;

import cs324.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.*;

public class BootstrapGrpcServer {
    private static final int PORT = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 50051;
    private static final long WORKER_TIMEOUT_MS = 6000;

    private static class BootstrapHandler extends BootstrapServiceGrpc.BootstrapServiceImplBase {
        private final Map<Integer, WorkerDetails> workers = new HashMap<>();
        private final Map<Integer, Long> lastSeen = new HashMap<>();
        private final Set<Integer> readyWorkers = new HashSet<>();
        private final int requiredWorkers;
        private boolean clusterStarted;

        BootstrapHandler(int requiredWorkers) {
            this.requiredWorkers = requiredWorkers;
        }

        private void removeExpired() {
            long now = System.currentTimeMillis();
            for (Integer id : new ArrayList<>(lastSeen.keySet())) {
                if (now - lastSeen.get(id) > WORKER_TIMEOUT_MS) {
                    workers.remove(id);
                    lastSeen.remove(id);
                    readyWorkers.remove(id);
                    System.out.println("Worker " + id + " timed out; its slot is now free.");
                }
            }
        }

        synchronized void checkWorkers() { removeExpired(); }

        @Override
        public synchronized void registerWorker(RegisterRequest request,
                StreamObserver<RegisterResponse> observer) {
            removeExpired();
            if (!request.hasWorker()) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription("Missing worker details").asRuntimeException());
                return;
            }
            WorkerDetails worker = request.getWorker();
            int id = worker.getWorkerId();
            if (id < 1 || id > requiredWorkers || worker.getHost().isBlank()
                    || worker.getPort() < 1 || worker.getPort() > 65535) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(
                        "Allowed IDs: 1 to " + requiredWorkers + "; use a valid host and port").asRuntimeException());
                return;
            }
            if (workers.containsKey(id)) {
                observer.onError(Status.ALREADY_EXISTS.withDescription("Worker " + id + " already registered").asRuntimeException());
                return;
            }
            for (WorkerDetails old : workers.values()) {
                if (old.getPort() == worker.getPort()
                        && old.getHost().equalsIgnoreCase(worker.getHost())) {
                    observer.onError(Status.ALREADY_EXISTS.withDescription("Port already in use").asRuntimeException());
                    return;
                }
            }
            RegisterResponse.Builder reply = RegisterResponse.newBuilder();
            List<WorkerDetails> candidates = new ArrayList<>(workers.values());
            if (!candidates.isEmpty()) {
                reply.setNeighbour(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size())));
            }
            workers.put(id, worker);
            lastSeen.put(id, System.currentTimeMillis());
            System.out.println("Worker " + id + " registered (" + workers.size() + "/" + requiredWorkers + ")");
            observer.onNext(reply.build());
            observer.onCompleted();
        }

        @Override
        public synchronized void unregisterWorker(UnregisterRequest request, StreamObserver<Empty> observer) {
            int id = request.getWorkerId();
            workers.remove(id);
            lastSeen.remove(id);
            readyWorkers.remove(id);
            observer.onNext(Empty.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public synchronized void heartbeat(HeartbeatRequest request, StreamObserver<Empty> observer) {
            if (!workers.containsKey(request.getWorkerId())) {
                observer.onError(Status.NOT_FOUND.withDescription("Worker must register first").asRuntimeException());
                return;
            }
            lastSeen.put(request.getWorkerId(), System.currentTimeMillis());
            observer.onNext(Empty.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public synchronized void getActiveWorkers(Empty request, StreamObserver<WorkerList> observer) {
            removeExpired();
            List<WorkerDetails> list = new ArrayList<>(workers.values());
            list.sort(Comparator.comparingInt(WorkerDetails::getWorkerId));
            observer.onNext(WorkerList.newBuilder().addAllWorkers(list).build());
            observer.onCompleted();
        }

        @Override
        public synchronized void getRandomActiveWorker(RandomWorkerRequest request,
                StreamObserver<RandomWorkerResponse> observer) {
            removeExpired();
            List<WorkerDetails> list = new ArrayList<>(workers.values());
            list.removeIf(w -> w.getWorkerId() == request.getExcludeWorkerId());
            RandomWorkerResponse.Builder reply = RandomWorkerResponse.newBuilder();
            if (!list.isEmpty()) reply.setWorker(list.get(ThreadLocalRandom.current().nextInt(list.size())));
            observer.onNext(reply.build());
            observer.onCompleted();
        }

        @Override
        public synchronized void workerReady(ReadyRequest request, StreamObserver<ReadyResponse> observer) {
            int id = request.getWorkerId();
            if (!workers.containsKey(id)) {
                observer.onError(Status.NOT_FOUND.withDescription("Worker isn't registered").asRuntimeException());
                return;
            }
            if (readyWorkers.add(id)) System.out.println("Worker " + id + " ready (" + readyWorkers.size() + "/" + requiredWorkers + ")");
            if (!clusterStarted && readyWorkers.size() == requiredWorkers) {
                clusterStarted = true;
                System.out.println("All workers ready. Worker nodes may now elect a coordinator.");
            }
            observer.onNext(ReadyResponse.newBuilder().setSuccess(true).build());
            observer.onCompleted();
        }

        @Override
        public synchronized void isElectionReady(Empty request, StreamObserver<ElectionStatus> observer) {
            observer.onNext(ElectionStatus.newBuilder().setReady(clusterStarted).build());
            observer.onCompleted();
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        int count;
        while (true) {
            System.out.print("Enter number of workers (3-10): ");
            if (!scanner.hasNextLine()) return;
            String input = scanner.nextLine().trim();
            if (input.matches("-[0-9]+")) {
                System.out.println("Error: Negative integers aren't allowed.");
                continue;
            }
            if (!input.matches("[0-9]+")) {
                System.out.println("Error: Enter a whole number (no text or symbols).");
                continue;
            }
            try { count = Integer.parseInt(input); }
            catch (NumberFormatException e) { count = 0; }
            if (count >= 3 && count <= 10) break;
            System.out.println("Error: Number of workers must be between 3 and 10.");
        }
        BootstrapHandler handler = new BootstrapHandler(count);
        Server server = ServerBuilder.forPort(PORT).addService(handler).build().start();
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(handler::checkWorkers, 2, 2, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            monitor.shutdownNow();
            server.shutdown();
        }));
        System.out.println("DistriLab gRPC Bootstrap running on port " + PORT);
        System.out.println("Waiting for " + count + " registered and ready workers...");
        server.awaitTermination();
    }
}
