package org.stateview;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

public class KafkaStateUtils {
    public static TupleSerializer<Tuple2<KafkaTopicPartition, Long>> createStateDescriptorSerializer(
        ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation
        // and allow to disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[]{
            new KryoSerializer<>(KafkaTopicPartition.class, executionConfig),
            LongSerializer.INSTANCE
        };
        @SuppressWarnings("unchecked")
        Class<Tuple2<KafkaTopicPartition, Long>> tupleClass = (Class<Tuple2<KafkaTopicPartition, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }

    public static TypeInformation<Tuple2<KafkaTopicPartition, Long>> createTypeInformation() {
        return TypeInformation.of(new TypeHint<Tuple2<KafkaTopicPartition, Long>>() {});
    }
}
