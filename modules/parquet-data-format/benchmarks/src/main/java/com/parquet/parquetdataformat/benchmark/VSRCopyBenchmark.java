    /*
     * SPDX-License-Identifier: Apache-2.0
     *
     * The OpenSearch Contributors require contributions made to
     * this file be licensed under the Apache-2.0 license or a
     * compatible open source license.
     */

    package com.parquet.parquetdataformat.benchmark;

    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.IntVector;
    import org.apache.arrow.vector.VarCharVector;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;
    import org.apache.arrow.vector.types.pojo.Schema;
    import org.openjdk.jmh.annotations.*;

    import java.util.*;
    import java.util.concurrent.TimeUnit;

    @Fork(1)
    @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @State(Scope.Benchmark)
    public class VSRCopyBenchmark {

        private static final int ITERATIONS = 10000;

        private BufferAllocator allocator;
        private VectorSchemaRoot targetVSR;
        private Schema schema;

        @Setup
        public void setup() {
            allocator = new RootAllocator();
            schema = new Schema(Arrays.asList(
                    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                    new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)
            ));
            targetVSR = VectorSchemaRoot.create(schema, allocator);
        }

        @TearDown
        public void tearDown() {
            targetVSR.close();
            allocator.close();
        }

        public void benchmarkVSRtoVSRCopy() {
            VectorSchemaRoot sourceVSR = VectorSchemaRoot.create(schema, allocator);
            for (int i = 0; i < ITERATIONS; i++) {

                ((IntVector) sourceVSR.getVector("id")).setSafe(0, 123);
                ((VarCharVector) sourceVSR.getVector("name")).setSafe(0, "test".getBytes());
                ((IntVector) sourceVSR.getVector("age")).setSafe(0, 30);
                sourceVSR.setRowCount(1);

                ((IntVector) targetVSR.getVector("id")).setSafe(i, ((IntVector) sourceVSR.getVector("id")).get(0));
                ((VarCharVector) targetVSR.getVector("name")).setSafe(i, ((VarCharVector) sourceVSR.getVector("name")).get(0));
                ((IntVector) targetVSR.getVector("age")).setSafe(i, ((IntVector) sourceVSR.getVector("age")).get(0));
                targetVSR.setRowCount(i + 1);

                sourceVSR.clear();
            }
        }

        @Benchmark
        public void benchmarkMaptoVSRCopy() {
            for (int i = 0; i < ITERATIONS; i++) {

                Map<String, Object> docMap = new HashMap<>();
                docMap.put("id", 123);
                docMap.put("name", "test");
                docMap.put("age", 30);

                ((IntVector) targetVSR.getVector("id")).setSafe(i, (Integer) docMap.get("id"));
                ((VarCharVector) targetVSR.getVector("name")).setSafe(i, ((String) docMap.get("name")).getBytes());
                ((IntVector) targetVSR.getVector("age")).setSafe(i, (Integer) docMap.get("age"));
                targetVSR.setRowCount(i + 1);

                docMap.clear();
            }
        }

        //@Benchmark
        public void benchmarkMaptoVSRCopy2() {
            Map<String, Object> docMap = new HashMap<>();
            for (int i = 0; i < ITERATIONS; i++) {

                docMap.put("id", 123);
                docMap.put("name", "test");
                docMap.put("age", 30);

                ((IntVector) targetVSR.getVector("id")).setSafe(i, 123);
                ((VarCharVector) targetVSR.getVector("name")).setSafe(i, "test".getBytes());
                ((IntVector) targetVSR.getVector("age")).setSafe(i, 30);
                targetVSR.setRowCount(i + 1);

                docMap.clear();
            }
        }

        //@Benchmark
        public void benchmarkMaptoVSRCopy3() {
            Object[] values = new Object[3];
            for (int i = 0; i < ITERATIONS; i++) {

                values[0] = 123;
                values[1] = "test".getBytes();
                values[2] = 30;

                ((IntVector) targetVSR.getVector("id")).setSafe(i, (Integer) values[0]);
                ((VarCharVector) targetVSR.getVector("name")).setSafe(i, (byte[]) values[1]);
                ((IntVector) targetVSR.getVector("age")).setSafe(i, (Integer) values[2]);
                targetVSR.setRowCount(i + 1);
            }
        }

        @Benchmark
        public void benchmark2DArrayToVSRCopy() {
            for (int i = 0; i < ITERATIONS; i++) {
                Object[][] values = new Object[3][3];
                values[0][0] = "id";
                values[1][0] = "name";
                values[2][0] = "age";

                values[0][1] = 123;
                values[1][1] = "test".getBytes();
                values[2][1] = 30;

                ((IntVector) targetVSR.getVector((String)values[0][0])).setSafe(i, (Integer) values[0][1]);
                ((VarCharVector) targetVSR.getVector((String)values[1][0])).setSafe(i, (byte[]) values[1][1]);
                ((IntVector) targetVSR.getVector((String)values[2][0])).setSafe(i, (Integer) values[2][1]);
                targetVSR.setRowCount(i + 1);
            }
            //System.out.println(targetVSR.contentToTSVString());
        }

        @Benchmark
        public void benchmarkListToVSRCopy() {
            for (int i = 0; i < ITERATIONS; i++) {
                List<List<Object>> values = new ArrayList<>();

                values.add(new ArrayList<>(Arrays.asList("id", 123)));
                values.add(new ArrayList<>(Arrays.asList("name", "test".getBytes())));
                values.add(new ArrayList<>(Arrays.asList("age", 30)));

                ((IntVector) targetVSR.getVector((String)values.get(0).get(0))).setSafe(i, (Integer) values.get(0).get(1));
                ((VarCharVector) targetVSR.getVector((String)values.get(1).get(0))).setSafe(i, (byte[]) values.get(1).get(1));
                ((IntVector) targetVSR.getVector((String)values.get(2).get(0))).setSafe(i, (Integer) values.get(2).get(1));
                targetVSR.setRowCount(i + 1);
            }
            //System.out.println(targetVSR.contentToTSVString());
        }

        @Benchmark
        public void benchmarkListToVSRCopy1() {
            for (int i = 0; i < ITERATIONS; i++) {
                List<List<Object>> values = new ArrayList<>();

                values.add(new ArrayList<>(List.of("id", 123)));
                values.add(new ArrayList<>(List.of("name", "test".getBytes())));
                values.add(new ArrayList<>(List.of("age", 30)));

                ((IntVector) targetVSR.getVector((String)values.get(0).get(0))).setSafe(i, (Integer) values.get(0).get(1));
                ((VarCharVector) targetVSR.getVector((String)values.get(1).get(0))).setSafe(i, (byte[]) values.get(1).get(1));
                ((IntVector) targetVSR.getVector((String)values.get(2).get(0))).setSafe(i, (Integer) values.get(2).get(1));
                targetVSR.setRowCount(i + 1);
            }
            //System.out.println(targetVSR.contentToTSVString());
        }

        @Benchmark
        public void benchmarkListToVSRCopy2() {
            for (int i = 0; i < ITERATIONS; i++) {
                List<List<Object>> values = new ArrayList<>();

                List<Object> value1 = new ArrayList<>();
                value1.add("id");
                value1.add(123);
                values.add(value1);
                List<Object> value2 = new ArrayList<>();
                value2.add("name");
                value2.add("test".getBytes());
                values.add(value2);
                List<Object> value3 = new ArrayList<>();
                value3.add("age");
                value3.add(30);
                values.add(value3);

                ((IntVector) targetVSR.getVector((String)values.get(0).get(0))).setSafe(i, (Integer) values.get(0).get(1));
                ((VarCharVector) targetVSR.getVector((String)values.get(1).get(0))).setSafe(i, (byte[]) values.get(1).get(1));
                ((IntVector) targetVSR.getVector((String)values.get(2).get(0))).setSafe(i, (Integer) values.get(2).get(1));
                targetVSR.setRowCount(i + 1);
            }
            //System.out.println(targetVSR.contentToTSVString());
        }

        @Benchmark
        public void benchmarkDirectWrite() {
            for (int i = 0; i < ITERATIONS; i++) {
                ((IntVector) targetVSR.getVector("id")).setSafe(i, 123);
                ((VarCharVector) targetVSR.getVector("name")).setSafe(i, "test".getBytes());
                ((IntVector) targetVSR.getVector("age")).setSafe(i, 30);
                targetVSR.setRowCount(i + 1);
            }
            //System.out.println(targetVSR.contentToTSVString());
        }
    }