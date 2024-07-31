package org.stateview;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.BroadcastStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class SavePointParserTest {
    private static final int FILE_STATE_SIZE = 1;

    private static final String ACCOUNT_UID = "accounts";

    private static final String CURRENCY_UID = "currency";

    private static final String MODIFY_UID = "numbers";

    private static final MapStateDescriptor<String, Double> descriptor =
        new MapStateDescriptor<>("currency-rate", Types.STRING, Types.DOUBLE);

    public static final File newFolder = new File("/Users/gezi/Dev/tmp/tmp");

    private static final Collection<Account> accounts =
        Arrays.asList(new Account(1, 100.0), new Account(2, 100.0), new Account(3, 100.0));

    private static final Collection<CurrencyRate> currencyRates =
        Arrays.asList(new CurrencyRate("USD", 1.0), new CurrencyRate("EUR", 1.3));


    public void testStateBootstrapAndModification(StateBackend backend) throws Exception {
        final String savepointPath = getTempDirPath(new AbstractID().toHexString());

        bootstrapState(backend, savepointPath);

        validateBootstrap(backend, savepointPath);

        final String modifyPath = getTempDirPath(new AbstractID().toHexString());

        modifySavepoint(backend, savepointPath, modifyPath);

        validateModification(backend, modifyPath);
    }

    private void bootstrapState(StateBackend backend, String savepointPath) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Account> accountDataSet = bEnv.fromCollection(accounts);

        BootstrapTransformation<Account> transformation =
            OperatorTransformation.bootstrapWith(accountDataSet)
                .keyBy(acc -> acc.id)
                .transform(new AccountBootstrapper());

        DataSet<CurrencyRate> currencyDataSet = bEnv.fromCollection(currencyRates);

        BootstrapTransformation<CurrencyRate> broadcastTransformation =
            OperatorTransformation.bootstrapWith(currencyDataSet)
                .transform(new CurrencyBootstrapFunction());

        Savepoint.create(backend, 128)
            .withOperator(ACCOUNT_UID, transformation)
            .withOperator(CURRENCY_UID, broadcastTransformation)
            .write(savepointPath);

        bEnv.execute("Bootstrap");
    }

    private void validateBootstrap(StateBackend backend, String savepointPath) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStateBackend(backend);

        DataStream<Account> stream =
            sEnv.fromCollection(accounts)
                .keyBy(acc -> acc.id)
                .flatMap(new UpdateAndGetAccount())
                .uid(ACCOUNT_UID);


        sEnv.fromCollection(currencyRates)
            .connect(sEnv.fromCollection(currencyRates).broadcast(descriptor))
            .process(new CurrencyValidationFunction())
            .uid(CURRENCY_UID)
            .addSink(new DiscardingSink<>());

        JobGraph jobGraph = sEnv.getStreamGraph().getJobGraph();
        jobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(savepointPath, false));
    }

    private void modifySavepoint(StateBackend backend, String savepointPath, String modifyPath)
        throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> data = bEnv.fromElements(1, 2, 3);

        BootstrapTransformation<Integer> transformation =
            OperatorTransformation.bootstrapWith(data).transform(new ModifyProcessFunction());

        Savepoint.load(bEnv, savepointPath, backend)
            .removeOperator(CURRENCY_UID)
            .withOperator(MODIFY_UID, transformation)
            .write(modifyPath);

        bEnv.execute("Modifying");
    }

    private void validateModification(StateBackend backend, String savepointPath) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStateBackend(backend);

        DataStream<Account> stream =
            sEnv.fromCollection(accounts)
                .keyBy(acc -> acc.id)
                .flatMap(new UpdateAndGetAccount())
                .uid(ACCOUNT_UID);

        stream.map(acc -> acc.id)
            .map(new StatefulOperator())
            .uid(MODIFY_UID)
            .addSink(new DiscardingSink<>());

        JobGraph jobGraph = sEnv.getStreamGraph().getJobGraph();
        jobGraph.setSavepointRestoreSettings(
            SavepointRestoreSettings.forPath(savepointPath, false));
    }

    /** A simple pojo. */
    @SuppressWarnings("WeakerAccess")
    public static class Account {
        Account(int id, double amount) {
            this.id = id;
            this.amount = amount;
            this.timestamp = 1000L;
        }

        public int id;

        public double amount;

        public long timestamp;

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Account
                && ((Account) obj).id == id
                && ((Account) obj).amount == amount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, amount);
        }
    }

    /** A simple pojo. */
    @SuppressWarnings("WeakerAccess")
    public static class CurrencyRate {
        public String currency;

        public Double rate;

        CurrencyRate(String currency, double rate) {
            this.currency = currency;
            this.rate = rate;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CurrencyRate
                && ((CurrencyRate) obj).currency.equals(currency)
                && ((CurrencyRate) obj).rate.equals(rate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(currency, rate);
        }
    }

    /** A savepoint writer function. */
    public static class AccountBootstrapper extends KeyedStateBootstrapFunction<Integer, Account> {
        ValueState<Double> state;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>("total", Types.DOUBLE);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Account value, Context ctx) throws Exception {
            state.update(value.amount);
        }
    }

    /** A streaming function bootstrapped off the state. */
    public static class UpdateAndGetAccount extends RichFlatMapFunction<Account, Account> {
        ValueState<Double> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>("total", Types.DOUBLE);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Account value, Collector<Account> out) throws Exception {
            Double current = state.value();
            if (current != null) {
                value.amount += current;
            }

            state.update(value.amount);
            out.collect(value);
        }
    }

    /** A bootstrap function. */
    public static class ModifyProcessFunction extends StateBootstrapFunction<Integer> {
        List<Integer> numbers;

        ListState<Integer> state;

        @Override
        public void open(Configuration parameters) {
            numbers = new ArrayList<>();
        }

        @Override
        public void processElement(Integer value, Context ctx) {
            numbers.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.addAll(numbers);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                context.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<>("numbers", Types.INT));
        }
    }

    /** A streaming function bootstrapped off the state. */
    public static class StatefulOperator extends RichMapFunction<Integer, Integer>
        implements CheckpointedFunction {
        List<Integer> numbers;

        ListState<Integer> state;

        @Override
        public void open(Configuration parameters) {
            numbers = new ArrayList<>();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.addAll(numbers);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                context.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<>("numbers", Types.INT));

            if (context.isRestored()) {
                Set<Integer> expected = new HashSet<>();
                expected.add(1);
                expected.add(2);
                expected.add(3);

                for (Integer number : state.get()) {
                    Assert.assertTrue("Duplicate state", expected.contains(number));
                    expected.remove(number);
                }

                Assert.assertTrue(
                    "Failed to bootstrap all state elements: "
                        + Arrays.toString(expected.toArray()),
                    expected.isEmpty());
            }
        }

        @Override
        public Integer map(Integer value) {
            return null;
        }
    }

    /** A broadcast bootstrap function. */
    public static class CurrencyBootstrapFunction
        extends BroadcastStateBootstrapFunction<CurrencyRate> {

        @Override
        public void processElement(CurrencyRate value, Context ctx) throws Exception {
            ctx.getBroadcastState(descriptor).put(value.currency, value.rate);
        }
    }

    /** Checks the restored broadcast state. */
    public static class CurrencyValidationFunction
        extends BroadcastProcessFunction<CurrencyRate, CurrencyRate, Void> {

        @Override
        public void processElement(CurrencyRate value, ReadOnlyContext ctx, Collector<Void> out)
            throws Exception {
            Assert.assertEquals(
                "Incorrect currency rate",
                value.rate,
                ctx.getBroadcastState(descriptor).get(value.currency),
                0.0001);
        }

        @Override
        public void processBroadcastElement(CurrencyRate value, Context ctx, Collector<Void> out) {
            // ignore
        }
    }

    public String getTempDirPath(String dirName) throws IOException {
        File f = createAndRegisterTempFile(dirName);
        return f.toURI().toString();
    }

    public String getTempFilePath(String fileName) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        return f.toURI().toString();
    }

    public String createTempFile(String fileName, String contents) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        FileUtils.writeFileUtf8(f, contents);
        return f.toURI().toString();
    }

    public File createAndRegisterTempFile(String fileName) throws IOException {
        return new File(newFolder, fileName);
    }

}