package org.stateview;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.RegisteredOperatorStateBackendMetaInfo;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 用于解析和存储 Flink Savepoint 的工具类.
 */
public class SavePointParser {

    /**
     * 从指定路径加载并解析 Savepoint.
     *
     * @param path       Savepoint 文件的路径.
     * @return {@link SavepointContent}  包含解析后的 Savepoint 数据.
     * @throws Exception 如果在加载或解析 Savepoint 期间发生错误.
     */
    public static SavepointContent loadAndParseSavepoint(String path) throws Exception {
        CheckpointMetadata savepoint = SavepointLoader.loadSavepointMetadata(path);
        return parseSavepoint(savepoint, (id -> true));
    }

    public static SavepointContent loadAndParseSavepoint(String path, String operatorIDHash) throws Exception {
        CheckpointMetadata savepoint = SavepointLoader.loadSavepointMetadata(path);
        return parseSavepoint(savepoint, (id -> id.toHexString().equals(operatorIDHash)));
    }

    /**
     * 解析 Savepoint 元数据.
     *
     * @param savepoint  Savepoint 元数据.
     * @param operatorFilter 用于判断是否解析该 Operator 的过滤器.
     * @return  {@link SavepointContent} 包含解析后的 Savepoint 数据.
     * @throws Exception 如果在解析 Savepoint 期间发生错误.
     */
    public static SavepointContent parseSavepoint(CheckpointMetadata savepoint, Predicate<OperatorID> operatorFilter) throws Exception {
        SavepointContent content = new SavepointContent(savepoint.getCheckpointId());

        // 迭代 Savepoint 中的 operator 状态
        for (OperatorState operatorState : savepoint.getOperatorStates()) {
            OperatorID stateOperatorID = operatorState.getOperatorID();

            // 使用过滤器判断是否解析该 Operator
            if (!operatorFilter.test(stateOperatorID)) {
                continue;
            }

            // 迭代 operator 的 subtask 状态
            Map<Integer, OperatorSubtaskState> subtaskStates = operatorState.getSubtaskStates();
            for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStates.entrySet()) {
                OperatorSubtaskState subtaskState = entry.getValue();

                // 解析并存储 managed operator state
                content.addManagedOperatorState(
                    stateOperatorID.toHexString(), parseManagedOpState(subtaskState.getManagedOperatorState()));

                // 解析并存储 managed keyed state
                content.addManagedKeyedState(
                    stateOperatorID.toHexString(), parseManagedKeyedState(subtaskState.getManagedKeyedState()));
            }
        }
        return content;
    }

    /**
     * 解析 ManagedKeyedState.
     *
     * @param managedKeyedState  ManagedKeyedState 集合.
     * @return {@link List<ManagedKeyedStateContent>} 包含解析后的 ManagedKeyedState 数据.
     * @throws Exception 如果解析状态时出错.
     */
    private static List<ManagedKeyedStateContent> parseManagedKeyedState(
        StateObjectCollection<KeyedStateHandle> managedKeyedState) throws Exception {
        List<ManagedKeyedStateContent> keyedStateContents = new ArrayList<>();
        for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
            KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
            KeyGroupRange keyGroupRange = keyGroupsStateHandle.getKeyGroupRange();

            try (FSDataInputStream in = keyGroupsStateHandle.openInputStream()) {
                DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(in);
                KeyedBackendSerializationProxy<Object> serializationProxy =
                    new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader());
                serializationProxy.read(inView);

                keyedStateContents.add(new ManagedKeyedStateContent(keyGroupRange,
                    serializationProxy.getStateMetaInfoSnapshots()));
            } catch (Exception e) {
                System.out.println("Error parsing keyed state: " + e.getMessage());
                keyedStateContents.add(new ManagedKeyedStateContent(keyGroupRange, new ArrayList<>()));
            }
        }
        return keyedStateContents;
    }

    /**
     * 解析 ManagedOperatorState.
     *
     * @param managedOperatorState  ManagedOperatorState 集合.
     * @return  {@link List<OperatorStateContent>} 包含解析后的 OperatorState 数据.
     * @throws Exception  如果解析状态时出错.
     */
    private static List<OperatorStateContent> parseManagedOpState(
        StateObjectCollection<OperatorStateHandle> managedOperatorState) throws Exception {
        List<OperatorStateContent> contents = new ArrayList<>();
        for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
            contents.addAll(parseOperatorStateHandle(operatorStateHandle));
        }
        return contents;
    }

    /**
     * 解析 OperatorStateHandle 的内容.
     *
     * @param operatorStateHandle  要解析的 OperatorStateHandle.
     * @return  {@link List<OperatorStateContent>}  表示已解析状态的 OperatorStateContent 对象列表.
     * @throws Exception  如果解析过程中出错.
     */
    private static List<OperatorStateContent> parseOperatorStateHandle(OperatorStateHandle operatorStateHandle) throws Exception {
        List<OperatorStateContent> contents = new ArrayList<>();
        // 创建用于反序列化的序列化代理
        OperatorBackendSerializationProxy serializationProxy =
            new OperatorBackendSerializationProxy(Thread.currentThread().getContextClassLoader());

        // 从输入流反序列化状态
        serializationProxy.read(
            new DataInputViewStreamWrapper(operatorStateHandle.openInputStream()));

        // 创建状态名称与其对应的序列化器的映射
        Map<String, TypeSerializer<?>> nameToSerializer =
            serializationProxy.getOperatorStateMetaInfoSnapshots().stream()
                .map(
                    stateMetaInfoSnapshot -> {
                        RegisteredOperatorStateBackendMetaInfo<Object> restoredMetaInfo =
                            new RegisteredOperatorStateBackendMetaInfo<>(stateMetaInfoSnapshot);
                        return new AbstractMap.SimpleImmutableEntry<>(
                            restoredMetaInfo.getName(), restoredMetaInfo.getPartitionStateSerializer());
                    })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // 获取状态名称及其分区偏移量
        Set<Map.Entry<String, OperatorStateHandle.StateMetaInfo>> entrySet = operatorStateHandle.getStateNameToPartitionOffsets().entrySet();

        // 迭代每个状态并反序列化其值
        try (FSDataInputStream in = operatorStateHandle.openInputStream()) {
            for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : entrySet) {
                deserializeOperateHandle(entry, in, nameToSerializer, contents);
            }
        }
        return contents;
    }

    private static void deserializeOperateHandle(Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry, FSDataInputStream in, Map<String, TypeSerializer<?>> nameToSerializer, List<OperatorStateContent> contents) throws IOException {
        String name = entry.getKey();
        OperatorStateHandle.StateMetaInfo metaInfo = entry.getValue();
        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(in);
        TypeSerializer<?> serializer = nameToSerializer.get(name);
        long[] offsets = metaInfo.getOffsets();
        List<Object> values = new ArrayList<>();

        // 反序列化每个分区的值
        for (long offset : offsets) {
            in.seek(offset);
            Object value = serializer.deserialize(inView);
            values.add(value);
        }
        contents.add(new OperatorStateContent(name, serializer, offsets, values));
    }
}



