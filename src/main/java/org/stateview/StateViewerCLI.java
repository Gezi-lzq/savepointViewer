package org.stateview;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
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
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

@Command(name = "state-viewer", mixinStandardHelpOptions = true, version = "1.0",
    subcommands = {StateViewerCLI.ViewCommand.class, StateViewerCLI.UpdateCommand.class},
    description = "View, delete and update states in Flink Savepoint.")
public class StateViewerCLI implements Callable<Integer> {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new StateViewerCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        CommandLine.usage(this, System.out);
        return 0;
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

    @Command(name = "view", description = "View the state of a specific operator in the Savepoint.")
    static class ViewCommand implements Callable<Integer> {
        @Parameters(index = "0", description = "Savepoint file path.")
        private String savepointPath;

        @CommandLine.Option(names = {"-uid", "--operator-uid"}, description = "Operator UID.", required = false)
        protected String operatorUid;

        @CommandLine.Option(names = {"-uidhash", "--operator-uidhash"}, description = "Operator UID Hash.", required = false)
        protected String operatorUidHash;

        @Override
        public Integer call() throws Exception {
            // Ensure only one of -uid or -uidhash is provided
            if ((operatorUid != null) && (operatorUidHash != null)) {
                throw new IllegalArgumentException("Please provide either -uid or -uidhash, but not both.");
            }
            SavepointContent savepointContent;
            String uidHash;
            if (operatorUidHash == null && operatorUid == null) {
                savepointContent = SavePointParser.loadAndParseSavepoint(savepointPath);
            } else {
                uidHash = operatorUidHash != null ? operatorUidHash : OperatorIDGenerator.fromUid(operatorUid).toHexString();
                savepointContent = SavePointParser.loadAndParseSavepoint(savepointPath, uidHash);
            }
            System.out.println(savepointContent);
            return 0;
        }
    }

    @Command(name = "removeAndUpdate", description = "Update the state of a specific operator in the Savepoint.")
    static class UpdateCommand implements Callable<Integer> {
        @Parameters(index = "0", description = "Savepoint file path.")
        private String savepointPath;

        @CommandLine.Option(names = {"-uid", "--operator-uid"}, description = "Operator UID.", required = false)
        protected String operatorUid;

        @CommandLine.Option(names = {"-uidhash", "--operator-uidhash"}, description = "Operator UID Hash.", required = false)
        protected String operatorUidHash;

        // Other parameters, such as new Operator ID, new state, etc.

        @Override
        public Integer call() throws Exception {
            // Ensure only one of -uid or -uidhash is provided
            if (((operatorUid == null) && (operatorUidHash == null)) ||
                ((operatorUid != null) && (operatorUidHash != null))) {
                throw new IllegalArgumentException("Please provide either -uid or -uidhash, but not both.");
            }
            File file = new File(savepointPath);
            String newSavepointPath = file.getParent() + "/" + file.getName() + "-new";
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // Create new Operator Transformation
            BootstrapTransformation<Tuple2<KafkaTopicPartition, Long>> bootstrapTransformation = OperatorTransformation
                .bootstrapWith(env.fromElements(Tuple2.of(new KafkaTopicPartition("topic", 0), 0L)))
                .transform(new EmptyStateBootstrapFunction<>());
            ExistingSavepoint existingSavepoint = Savepoint.load(env, savepointPath, new FsStateBackend(file.toURI()));
            // Update Savepoint
            existingSavepoint
                .removeOperator(operatorUid)
                // TODO - Add new operator with new state
                .withOperator("tmp", bootstrapTransformation)
                .write(newSavepointPath);
            env.execute();
            System.out.println("Operator state updated successfully. New savepoint is saved at " + newSavepointPath);
            return 0;
        }
    }
}