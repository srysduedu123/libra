package client;

import common.entity.TaskAndWorker;
import common.exception.OperationOutOfDateException;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-9
 * Time: 下午8:03
 * To change this template use File | Settings | File Templates.
 */
public interface IRebalancer {
    public List<String> calculateMyTask(String myId, List<TaskAndWorker> immutableTaskAndWorkerList, List<String> immutableWorkerList) throws OperationOutOfDateException;
}
