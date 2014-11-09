package cn.edu.sdu.cs.starry.libra.client;

import cn.edu.sdu.cs.starry.libra.common.exception.OperationOutOfDateException;

import java.util.List;

/**
 * @author xccui
 * Date: 13-10-9
 * Time: 8:03
 */
public interface IRebalanceTool {
    public List<String> calculateMyTask(String myId, final List<String> taskList, final List<String> workerList);
}
