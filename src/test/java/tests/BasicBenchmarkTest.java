package tests;

import lib.util.persistent.*;
import lib.util.persistent.spi.PersistentMemoryProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class BasicBenchmarkTest {
    private final static boolean enableTimerTest = true;
    private final static boolean enableAssert = false;
    private final static boolean enableVerify = false;
    private static boolean verbose = false;
    private final static long maxKeyCount = 100000;
    private final static int keySize = 16;
    private final static int valueSize = 128;
    private static volatile String keyPrefix;
    private static volatile String valuePrefix;

    private static PersistentString magicInitValue;
    private static PersistentString magicUpdateValue;

    public static void main (String[] args) {
        run();
    }
    private static void buildValues() {
        StringBuilder keyPrefixBuilder = new StringBuilder();
        StringBuilder valuePrefixBuilder = new StringBuilder();
        for (int i = 0; i < keySize; i++) {
            keyPrefixBuilder.append('K');
        }
        keyPrefix = keyPrefixBuilder.toString();
        for (int i = 0; i < valueSize; i++) {
            valuePrefixBuilder.append('V');
        }
        valuePrefix = valuePrefixBuilder.toString();
        assert keyPrefix != null;
        assert valuePrefix != null;

        magicInitValue = new PersistentString(valuePrefix);
        magicUpdateValue = new PersistentString("UPD" + valuePrefix);

    }

    public static boolean run() {
        PersistentMemoryProvider.getDefaultProvider().getHeap().open();
        System.out.println("****************BasicBenchmarkTest Tests**********************");
        buildValues();

        BasicBenchmarkTest test = new BasicBenchmarkTest();
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
    private static PersistentSkipListMap<PersistentString, PersistentString> getPersistentSkipListMap() {
        String id = threadSafeId("tests.PersistentSkipListMap");
        PersistentSkipListMap<PersistentString, PersistentString> map = ObjectDirectory.get(id, PersistentSkipListMap.class);
        if(map == null) {
            map = new PersistentSkipListMap<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentSkipListMap2<PersistentString, PersistentString> getPersistentSkipListMap2() {
        String id = threadSafeId("tests.PersistentSkipListMap2");
        PersistentSkipListMap2<PersistentString, PersistentString> map = ObjectDirectory.get(id,PersistentSkipListMap2.class);
        if(map == null) {
            map = new PersistentSkipListMap2<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentFPTree1<PersistentString, PersistentString> getPersistentFPTree1() {
        String id = threadSafeId("tests.PersistentFPTree1");
        PersistentFPTree1<PersistentString, PersistentString> map = ObjectDirectory.get(id,PersistentFPTree1.class);
        if(map == null) {
            map = new PersistentFPTree1<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static PersistentFPTree2<PersistentString, PersistentString> getPersistentFPTree2() {
        String id = threadSafeId("tests.PersistentFPTree2");
        PersistentFPTree2<PersistentString, PersistentString> map = ObjectDirectory.get(id,PersistentFPTree2.class);
        if(map == null) {
            map = new PersistentFPTree2<>();
            ObjectDirectory.put(id, map);
        }
        return map;
    }

    public boolean testBenchmarkCounterBased(String className, PersistentSortedMap<PersistentString, PersistentString> map) {
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
                PersistentString key = new PersistentString(keyPrefix + l);
                PersistentString val = new PersistentString(valuePrefix + l);
                PersistentString out = map.put(key, val);
                if (out != null) {
                    System.out.println(String.format("CREATING key %s failed, out = %s", keyPrefix + l, out.toString()));
                }
                assert(out == null);
            }
            latencies.put("CREATE", (System.nanoTime() - start) / LoopCount);

            // GET
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                PersistentString key = new PersistentString(keyPrefix + l);
                assert(map.get(key).toString().equals(valuePrefix + l));
            }
            latencies.put("GET", (System.nanoTime() - start) / LoopCount);

            // UPDATE
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                PersistentString key = new PersistentString(keyPrefix + l);
                PersistentString out = map.put(key, magicUpdateValue);
                assert(out.toString().equals(valuePrefix + l));
                if (enableVerify) {
                    assert map.get(key).equals(magicUpdateValue);
                }
            }
            latencies.put("UPDATE", (System.nanoTime() - start) / LoopCount);

            // DELETE
            start = System.nanoTime();
            for (int l = 0; l < LoopCount; l++) {
                PersistentString key = new PersistentString(keyPrefix + l);
                map.remove(key);
                if (enableVerify) {
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

    public boolean testBenchmarkTimeBound(final String className, final PersistentSortedMap<PersistentString, PersistentString> map) {
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
            Thread t1 = new Thread(new TimerTestTask("CREATE", stop, latencies, (k) -> map.put(new PersistentString(k), magicInitValue) ));
            startWaitandThenStop(t1, timeSecond * 1000, stop);

            // GET:
            Thread t2 = new Thread(new TimerTestTask("GET", stop, latencies, (k) -> map.get(new PersistentString(k)) ));
            startWaitandThenStop(t2, timeSecond * 1000, stop);

            // UPDATE:
            Thread t3 = new Thread(new TimerTestTask("UPDATE", stop, latencies, (k) -> map.put(new PersistentString(k), magicUpdateValue) ));
            startWaitandThenStop(t3, timeSecond * 1000, stop);

            // DELETE:
            Thread t4 = new Thread(new TimerTestTask("DELETE", stop, latencies, (k) -> map.remove(new PersistentString(k)) ));
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
        private Function<String, PersistentString> fn;
        TimerTestTask(final String name, final AtomicBoolean stop, final Map<String, Long> latencies,
                         Function<String, PersistentString> fn) {
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
                final String keyId = keyPrefix + (loopCount % maxKeyCount);
                PersistentString out = this.fn.apply(keyId);
                loopCount++;
                if ("CREATE".equals(name)) {
                    if (out != null) {
                        errorCount++;
                        if (verbose) {
                            System.out.println(String.format("Failed to %s key %s, out = %s", name, keyId, out.toString()));
                        }
                    }
                    if (enableAssert) {
                        assert (out == null);
                    }
                } else {
                    if (out == null) {
                        errorCount++;
                        if (verbose) {
                            System.out.println(String.format("Failed to %s key %s", name, keyId));
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
