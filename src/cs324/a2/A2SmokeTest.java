package cs324.a2;

import cs324.client.GrpcClientConnection;
import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.JobProgress;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Enhanced A2 Smoke Test verifying gRPC streaming job progress, 
 * completion metrics, and cluster calculation accuracy for MAX, PRIMESUM, and PRIMECOUNT.
 * 
 * Start Bootstrap and 3-10 gRPC workers before running this test.
 */
public class A2SmokeTest {

    private static final long STREAM_TIMEOUT_SECONDS = 30;

    private static void check(JobRequest job, BigInteger expected) throws Exception {
        AtomicReference<BigInteger> answer = new AtomicReference<>();
        AtomicInteger maxProgress = new AtomicInteger(0);
        AtomicInteger updates = new AtomicInteger();
        long startTime = System.currentTimeMillis();

        System.out.println("[TEST] Submitting job: " + job.getJobType() + " ...");

        GrpcClientConnection.stream(job, update -> {
            updates.incrementAndGet();
            int pct = update.getPercent();
            
            if (pct < 0 || pct > 100 || update.getCompleted() > update.getTotal()) {
                throw new AssertionError("Invalid progress received for " + job.getJobType() 
                        + " [percent=" + pct + ", completed=" + update.getCompleted() 
                        + ", total=" + update.getTotal() + "]");
            }
            
            maxProgress.accumulateAndGet(pct, Math::max);
            System.out.println("  -> " + job.getJobType() + " Progress: " + pct + "% ("
                    + update.getCompleted() + "/" + update.getTotal() + ")");
                    
            if (update.getFinished()) {
                if (pct != 100) {
                    throw new AssertionError("Job marked finished before reaching 100% progress.");
                }
                answer.set(new BigInteger(update.getResult()));
            }
        });

        // Verify result validity
        if (answer.get() == null) {
            throw new AssertionError(job.getJobType() + " failed to return a result payload.");
        }
        if (!expected.equals(answer.get())) {
            throw new AssertionError(job.getJobType() + " expected " + expected
                    + " but got " + answer.get());
        }
        if (updates.get() < 1 || maxProgress.get() != 100) {
            throw new AssertionError("Insufficient or incomplete progress events received for " + job.getJobType());
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("PASS " + job.getJobType() + " = " + answer.get() + " [" + duration + "ms]");
    }

    public static void main(String[] args) {
        System.out.println("=========================================");
        System.out.println("    STARTING DISTRILAB A2 SMOKE TESTS    ");
        System.out.println("=========================================");

        try {
            List<BigInteger> huge = Arrays.asList(
                    new BigInteger("1000000000000000000000000000000000000"),
                    new BigInteger("9999999999999999999999999999999999999"),
                    BigInteger.valueOf(50));

            check(new JobRequest(JobType.MAX, huge), huge.get(1));
            check(new JobRequest(JobType.PRIMESUM, 2, 10), BigInteger.valueOf(17));
            check(new JobRequest(JobType.PRIMECOUNT, Arrays.asList(2, 3, 5, 7, 8, 9)),
                    BigInteger.valueOf(4));

            System.out.println("=========================================");
            System.out.println("      ALL A2 SMOKE TESTS PASSED          ");
            System.out.println("=========================================");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("❌ A2 SMOKE TEST FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}