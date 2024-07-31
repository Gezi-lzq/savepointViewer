package org.stateview;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.Collection;

/**
 * 表示解析后的 ManagedKeyedStateContent 的数据结构.
 */
class ManagedKeyedStateContent {
    private final KeyGroupRange keyGroupRange;
    private final Collection<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

    public ManagedKeyedStateContent(KeyGroupRange keyGroupRange,
                                    Collection<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
        this.keyGroupRange = keyGroupRange;
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    public Collection<StateMetaInfoSnapshot> getStateMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    @Override
    public String toString() {
        return "\t\t\tKey Group Range: " + keyGroupRange + "\n" +
            "\t\t\tState Meta Info Snapshots: " + stateMetaInfoSnapshots;
    }
}
