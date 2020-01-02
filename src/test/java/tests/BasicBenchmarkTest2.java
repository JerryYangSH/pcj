package tests;

import lib.util.persistent.*;
import lib.util.persistent.helper.PersistentDecoratedKey;
import lib.util.persistent.spi.PersistentMemoryProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class BasicBenchmarkTest2 {
    private final static boolean enableTimerTest = true;
    private final static boolean enableAssert = true;
    private final static boolean enableVerify = true;
    private static boolean verbose = false;
    private final static long maxKeyCount = 100000;
    private final static int keySize = 16;
    private final static int valueSize = 128;
    private static volatile byte[] keyPrefix = new byte[keySize];
    private static volatile byte[] valuePrefix = new byte[valueSize];
    private static volatile byte[] value2Prefix = new byte[valueSize];

    private static PersistentImmutableByteArray magicInitValue;
    private static PersistentImmutableByteArray magicUpdateValue;

    public static void main (String[] args) {
        run();
    }
    private static void buildValues() {

        Arrays.fill(keyPrefix, (byte)'K');
        Arrays.fill(valuePrefix, (byte)'V');
        Arrays.fill(value2Prefix, (byte)'U');

        magicInitValue = new PersistentImmutableByteArray(valuePrefix);
        magicUpdateValue = new PersistentImmutableByteArray(value2Prefix);

    }

    public static boolean run() {
        PersistentMemoryProvider.getDefaultProvider().getHeap().open();
        System.out.println("****************BasicBenchmarkTest2 Tests**********************");
        buildValues();

        BasicBenchmarkTest2 test = new BasicBenchmarkTest2();
        return test.runTest();
    }

    private static String threadSafeId(String id) {
        return id + "_" + Thread.currentThread().getId();
    }

    public boolean runTest() {
        try {
            if (enableTimerTest) {
                //testBenchmarkTimeBound("PersistentSkipListMap", getPersistentSkipListMap());
                //testBenchmarkTimeBound("PersistentSkipListMap2", getPersistentSkipListMap2());
                //testBenchmarkTimeBound("PersistentFPTree1", getPersistentFPTree1());
                testBenchmarkTimeBound("PersistentFPTree2", getPersistentFPTree2());
            } else {
                //testBenchmarkCounterBased("PersistentSkipListMap", getPersistentSkipListMap());
                //testBenchmarkCounterBased("PersistentSkipListMap2", getPersistentSkipListMap2());
                //testBenchmarkCounterBased("PersistentFPTree1", getPersistentFPTree1());
                testBenchmarkCounterBased("PersistentFPTree2", getPersistentFPTree2());
            }
            return true;
        } catch (Exception e) {
            System.out.println(e);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static PersistentSkipListMap<PersistentImmutableByteArray, PersistentImmutableByteArray> getPersistentSkipListMap() {
        String id = threadSafeId("tests.PersistentSkipListMap");
        PersistentSkipListMap<PersistentImmutableByteArray, PersistentImmutableByteArray> map = ObjectDirectory.get(id, PersistentSkipListMap.class);
        if(map == null) {
            map = new PersistentSkipListMap<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentSkipListMap2<PersistentImmutableByteArray, PersistentImmutableByteArray> getPersistentSkipListMap2() {
        String id = threadSafeId("tests.PersistentSkipListMap2");
        PersistentSkipListMap2<PersistentImmutableByteArray, PersistentImmutableByteArray> map = ObjectDirectory.get(id,PersistentSkipListMap2.class);
        if(map == null) {
            map = new PersistentSkipListMap2<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentFPTree1<PersistentImmutableByteArray, PersistentImmutableByteArray> getPersistentFPTree1() {
        String id = threadSafeId("tests.PersistentFPTree1");
        PersistentFPTree1<PersistentImmutableByteArray, PersistentImmutableByteArray> map = ObjectDirectory.get(id,PersistentFPTree1.class);
        if(map == null) {
            map = new PersistentFPTree1<>(8, 64);
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentFPTree2<PersistentDecoratedKey, PersistentImmutableByteArray> getPersistentFPTree2() {
        String id = threadSafeId("tests.PersistentFPTree2");
        PersistentFPTree2<PersistentDecoratedKey, PersistentImmutableByteArray> map = ObjectDirectory.get(id,PersistentFPTree2.class);
        if(map == null) {
            map = new PersistentFPTree2<>(8, 64);
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    private void fillTail(final byte[] a, long l) {
        assert a.length >= 8;
        int len = a.length;
        for (int i = len - 1; i >= len - 8; i--) {
            byte b = (byte)(l & 0xff);
            l = l >> 8;
            a[i] = b;
        }
    }
    public boolean testBenchmarkCounterBased(String className, PersistentSortedMap<PersistentDecoratedKey, PersistentImmutableByteArray> map) {
        System.out.println(String.format("*Max Count Testing %s (Single Thread, %d keys)", className, maxKeyCount));
        assert className.equals(map.getClass().getSimpleName());

        assert (map != null);
        map.clear();
        assert(map.size() == 0);
        final int[] LoopCounts = new int[] {10000, 100000, 1000000};
        for (int LoopCount : LoopCounts) {
            System.out.println(String.format("[Key Count %6d] avg lat:", LoopCount));
            map.clear();
            assert(map.size() == 0);
            Map<String, Long> latencies = new HashMap<>();

            // CREATE:
            long start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                fillTail(keyPrefix, l);
                PersistentDecoratedKey key = new PersistentDecoratedKey(keyPrefix);
                PersistentImmutableByteArray out = map.put(key, magicInitValue);
                if (out != null) {
                    System.out.println(String.format("CREATING key %s failed, out = %s", keyPrefix, out.toString()));
                }
                assert(out == null);
            }
            latencies.put("CREATE", (System.nanoTime() - start) / LoopCount);

            // GET
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                fillTail(keyPrefix, l);
                PersistentDecoratedKey key = new PersistentDecoratedKey(keyPrefix);
                assert Arrays.equals(map.get(key).toArray(), magicInitValue.toArray());
            }
            latencies.put("GET", (System.nanoTime() - start) / LoopCount);

            // UPDATE
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                fillTail(keyPrefix, l);
                PersistentDecoratedKey key = new PersistentDecoratedKey(keyPrefix);
                PersistentImmutableByteArray out = map.put(key, magicUpdateValue);
                if (enableVerify) {
                    assert out != null;
                    assert Arrays.equals(out.toArray(), magicInitValue.toArray());
                    assert Arrays.equals(map.get(key).toArray(), magicUpdateValue.toArray());
                }
            }
            latencies.put("UPDATE", (System.nanoTime() - start) / LoopCount);

            // DELETE
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                fillTail(keyPrefix, l);
                PersistentDecoratedKey key = new PersistentDecoratedKey(keyPrefix);
                PersistentImmutableByteArray out = map.remove(key);
                if (enableVerify) {
                    assert out != null;
                    assert map.get(key) == null;
                }
            }
            latencies.put("DELETE", (System.nanoTime() - start) / LoopCount);

            for (Map.Entry<String, Long> lat : latencies.entrySet()) {
                System.out.println(String.format("\t%8s : %8d (us)", lat.getKey(), lat.getValue() / 1000));
            }
        }

        map.clear();
        return true;
    }

    private void startWaitandThenStop(Thread t, long ms, AtomicBoolean stop) {
        stop.set(false);
        t.start();
        try {
            Thread.sleep(ms);
            stop.set(true);
            t.join();
        } catch (InterruptedException ie) {
            System.out.println("CREATE test is interrupted : " + ie);
        }
    }

    public boolean testBenchmarkTimeBound(final String className, final PersistentSortedMap<PersistentDecoratedKey, PersistentImmutableByteArray> map) {
        System.out.println(String.format("*Time Bound Testing %s (Single Thread, %d keys)", className, maxKeyCount));
        assert className.equals(map.getClass().getSimpleName());

        assert (map != null);
        map.clear();
        assert(map.size() == 0);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final int[] timeSeconds = new int[] {10, 60, 300};
        for (int timeSecond : timeSeconds) {
            System.out.println(String.format("[Time Bound Test %6ds] avg lat:", timeSecond));
            map.clear();
            assert(map.size() == 0);
            Map<String, Long> latencies = new HashMap<>();

            // CREATE:
            Thread t1 = new Thread(new TimerTestTask("CREATE", stop, latencies, (k) -> map.put(k, magicInitValue) ));
            startWaitandThenStop(t1, timeSecond * 1000, stop);

            // GET:
            Thread t2 = new Thread(new TimerTestTask("GET", stop, latencies, (k) -> map.get(k)));
            startWaitandThenStop(t2, timeSecond * 1000, stop);

            // UPDATE:
            Thread t3 = new Thread(new TimerTestTask("UPDATE", stop, latencies, (k) -> map.put(k, magicUpdateValue) ));
            startWaitandThenStop(t3, timeSecond * 1000, stop);

            // DELETE:
            Thread t4 = new Thread(new TimerTestTask("DELETE", stop, latencies, (k) -> map.remove(k) ));
            startWaitandThenStop(t4, timeSecond * 1000, stop);

            for (Map.Entry<String, Long> lat : latencies.entrySet()) {
                System.out.println(String.format("\t%8s : %8d (us)", lat.getKey(), lat.getValue() / 1000));
            }
        }

        map.clear();
        return true;
    }

    class TimerTestTask implements Runnable {
        private String name;
        private AtomicBoolean stop;
        private Map<String, Long> latencies;
        private Function<PersistentDecoratedKey, PersistentImmutableByteArray> fn;
        TimerTestTask(final String name, final AtomicBoolean stop, final Map<String, Long> latencies,
                         Function<PersistentDecoratedKey, PersistentImmutableByteArray> fn) {
            this.name = name;
            this.stop = stop;
            this.latencies = latencies;
            this.fn = fn;
        }

        @Override
        public void run() {
            long loopCount = 0;
            long errorCount = 0;
            long start = System.nanoTime();
            while (true) {
                long keyId = loopCount % maxKeyCount;
                fillTail(keyPrefix, keyId);
                PersistentImmutableByteArray out = this.fn.apply(new PersistentDecoratedKey(keyPrefix));
                loopCount++;
                if ("CREATE".equals(name)) {
                    if (out != null) {
                        errorCount++;
                        if (verbose) {
                            System.out.println(String.format("Failed to %s key %d, out = %s", name, keyId, out.toString()));
                        }
                    }
                    if (enableAssert) {
                        assert (out == null);
                    }
                } else {
                    if (out == null) {
                        errorCount++;
                        if (verbose) {
                            System.out.println(String.format("Failed to %s key %d", name, keyId));
                        }
                    }
                    if (enableAssert) {
                        assert (out != null);
                    }
                }

                if ("CREATE".equals(name)) {
                    // Create enough keys before following read and update.
                    if (loopCount >= maxKeyCount) break;

                    // go on, ignore stop signal.
                    continue;
                } else if ("DELETE".equals(name)) {
                    // all keys are deleted, call it done
                    if (loopCount >= maxKeyCount) break;
                }
                if (stop.get()) { break; }
            }
            latencies.put(name, (System.nanoTime() - start) / loopCount);
            if (errorCount > 0) {
                System.out.println(String.format("%s had %d errors (%-2d%%)", name, errorCount, errorCount * 100 / loopCount));
            }
        }
    }
}
