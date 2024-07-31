package org.stateview;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表示解析后的 Savepoint 内容的数据结构.
 */
class SavepointContent {
    private final long checkpointId;
    private final Map<String, List<List<OperatorStateContent>>> managedOperatorStateContent;
    private final Map<String, List<List<ManagedKeyedStateContent>>> managedKeyedStateContent;
    public SavepointContent(long checkpointId) {
        this.checkpointId = checkpointId;
        this.managedOperatorStateContent = new HashMap<>();
        this.managedKeyedStateContent = new HashMap<>();
    }
    public void addManagedOperatorState(String operatorID, List<OperatorStateContent> content) {
        List<List<OperatorStateContent>> lists = this.managedOperatorStateContent.getOrDefault(operatorID, new ArrayList<>());
        lists.add(content);
        this.managedOperatorStateContent.put(operatorID, lists);
    }
    public void addManagedKeyedState(String operatorID, List<ManagedKeyedStateContent> content) {
        List<List<ManagedKeyedStateContent>> lists = this.managedKeyedStateContent.getOrDefault(operatorID, new ArrayList<>());
        lists.add(content);
        this.managedKeyedStateContent.put(operatorID, lists);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Checkpoint ID: ").append(checkpointId).append("\n");

        // 打印 Managed Operator State
        sb.append("Managed Operator State:\n");
        for (Map.Entry<String, List<List<OperatorStateContent>>> entry : managedOperatorStateContent.entrySet()) {
            sb.append("\toperatorID: ").append(entry.getKey()).append("\n");
            for (int i = 0; i < entry.getValue().size(); i++) {
                sb.append("\t\tSubtaskIndex: ").append(i).append("\n");
                List<OperatorStateContent> content = entry.getValue().get(i);
                for (OperatorStateContent stateContent : content) {
                    sb.append(stateContent.toString()).append("\n");
                }
            }
        }

        // 打印 Managed Keyed State
        sb.append("Managed Keyed State:\n");
        for (Map.Entry<String, List<List<ManagedKeyedStateContent>>> entry : managedKeyedStateContent.entrySet()) {
            sb.append("\toperatorID: ").append(entry.getKey()).append("\n");
            for (int i = 0; i < entry.getValue().size(); i++) {
                sb.append("\t\tSubtaskIndex: ").append(i).append("\n");
                List<ManagedKeyedStateContent> content = entry.getValue().get(i);
                for (ManagedKeyedStateContent stateContent : content) {
                    sb.append(stateContent.toString()).append("\n");
                }
            }
        }

        return sb.toString();
    }
}
