package org.stateview;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.AbstractID;

import java.io.File;
import java.io.IOException;

public class StateViewer {
    public static final File savepointDir = new File("/Users/gezi/Dev/tmp");
    public static final File savepointPath = new File(savepointDir, "savepoint-03503c-0b141632f106");

    public void main(String[] args) throws Exception {
        String path = savepointPath.toURI().toString();
        OperatorID operatorID = OperatorIDGenerator.fromUid("source");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(GenericData.Array.class,
            com.esotericsoftware.kryo.serializers.JavaSerializer.class);

        // Load and process the savepoint
        SavepointContent savepointContent = SavePointParser.loadAndParseSavepoint(path, operatorID.toHexString());

        System.out.println(savepointContent);

        DataSet<Tuple2<KafkaTopicPartition, Long>> topic = env.fromElements(Tuple2.of(new KafkaTopicPartition("topic", 0), 0L));

        BootstrapTransformation<Tuple2<KafkaTopicPartition, Long>> bootstrapTransformation = OperatorTransformation
            .bootstrapWith(topic)
            .transform(new EmptyStateBootstrapFunction<>());

        ExistingSavepoint existingSavepoint = Savepoint.load(env, path, new FsStateBackend(savepointDir.toURI()));
        String modifyPath = getNewDirPath(new AbstractID().toHexString());
        existingSavepoint.removeOperator("source")
            .withOperator("test", bootstrapTransformation)
            .write(modifyPath);
        env.execute();
    }

    public static String getNewDirPath(String dirName) throws IOException {
        File f = createAndRegisterTempFile(dirName);
        return f.toURI().toString();
    }
    public static File createAndRegisterTempFile(String fileName) throws IOException {
        return new File(savepointDir, fileName);
    }

    private static DataSet<Tuple2<KafkaTopicPartition, Long>> loadSavepoint(String path, String uid, String name) throws IOException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint existingSavepoint = Savepoint.load(env, path, new MemoryStateBackend());
        DataSet<Tuple2<KafkaTopicPartition, Long>> source =
            existingSavepoint.readUnionState(uid, name, KafkaStateUtils.createTypeInformation(),
                KafkaStateUtils.createStateDescriptorSerializer(env.getConfig()));
        return source;
    }

    public static class EmptyStateBootstrapFunction<T> extends StateBootstrapFunction<T> {
        @Override
        public void processElement(T value, Context ctx) throws Exception {
        }
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }
    }
}