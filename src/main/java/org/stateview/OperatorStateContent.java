package org.stateview;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;
import java.util.List;

/**
 * 表示解析后的 OperatorStateContent 的数据结构.
 */
class OperatorStateContent {
    private final String name;
    private final TypeSerializer<?> serializer;
    private final long[] offsets;
    private final List<Object> values;

    public OperatorStateContent(String name, TypeSerializer<?> serializer, long[] offsets, List<Object> values) {
        this.name = name;
        this.serializer = serializer;
        this.offsets = offsets;
        this.values = values;
    }

    public String getName() {
        return name;
    }

    public TypeSerializer<?> getSerializer() {
        return serializer;
    }

    public long[] getOffsets() {
        return offsets;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\t\t\t\tName: ").append(name)
            .append(", Serializer: ").append(serializer)
            .append(", Offsets: ").append(Arrays.toString(offsets));

        // 打印每个分区的值
        for (int i = 0; i < offsets.length; i++) {
            sb.append("\n\t\t\t\t\tOffset: ").append(offsets[i])
                .append(", Value: ").append(values.get(i));
        }
        return sb.toString();
    }
}
