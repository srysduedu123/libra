package client;

import common.entity.TaskAndWorker;
import common.exception.OperationOutOfDateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-12
 * Time: 下午9:17
 * To change this template use File | Settings | File Templates.
 */
public class DefaultRebalancer implements IRebalancer {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRebalancer.class);

    @Override
    public List<String> calculateMyTask(String myId, List<TaskAndWorker> immutableTaskAndWorkerList, List<String> immutableWorkerList) throws OperationOutOfDateException {
        if (immutableWorkerList.size() == 0) {
            return new ArrayList<>();
        }
        ArrayList<String> workerList = new ArrayList<>(immutableWorkerList.size());
        workerList.addAll(immutableWorkerList);
        Collections.sort(workerList);
        ArrayList<String> taskList = new ArrayList<>(immutableTaskAndWorkerList.size());
        for (TaskAndWorker tw : immutableTaskAndWorkerList) {
            taskList.add(tw.task);
        }
        int myOrder = workerList.indexOf(myId);
        if (-1 == myOrder) {
            throw new OperationOutOfDateException("Cannot find myId in workerList");
        }
        int workerNum = workerList.size();
        List<String> myTaskList = new LinkedList<>();
        for (int i = 0; i < taskList.size(); i++) {
            if (i % workerNum == myOrder) {
                myTaskList.add(taskList.get(i));
            }
        }
        System.out.print("Calculated my tasks:");
        for (String task : myTaskList) {
            System.out.print(task + ",");
        }
        System.out.println();
        return myTaskList;
    }


    private static class WorkerAndTaskList {
        String worker;
        LinkedList<String> taskList = new LinkedList<>();

        public WorkerAndTaskList(String worker) {
            this.worker = worker;
        }

    }

//    @Override
//    public List<String> calculateMyTask(String myId, List<TaskAndWorker> immutableTaskAndWorkerList, List<String> immutableWorkerList, List<String> myCurrentTask) throws OperationOutOfDateException {
//        if (null == myCurrentTask) {
//            myCurrentTask = new ArrayList<>(0);
//        }
//        LinkedList<TaskAndWorker> taskAndWorkerList = new LinkedList<>();
//        taskAndWorkerList.addAll(immutableTaskAndWorkerList);
//        Collections.sort(taskAndWorkerList);
//        int taskNum = taskAndWorkerList.size();
//        LOG.info("taskNum = " + taskNum);
//        if (0 == taskNum) {
//            return new ArrayList<>(0);
//        }
//        LinkedList<String> workerList = new LinkedList<>();
//        workerList.addAll(immutableWorkerList);
//        if (workerList.isEmpty()) {
//            throw new OperationOutOfDateException("workerList is empty");
//        }
//        System.out.print("Worker for rebalanced:");
//        for (String worker : workerList) {
//            System.out.print(worker + ",");
//        }
//        System.out.println();
//        int workerNum = workerList.size();
//        int minTaskPerWorker = taskNum / workerNum;
//        int numOfMoreTaskWorkers = taskNum % workerNum;
//        LOG.info("numOfMoreTaskWorkers = " + numOfMoreTaskWorkers);
//        int maxTaskPerWorker = 0 == numOfMoreTaskWorkers ? minTaskPerWorker : minTaskPerWorker + 1;
//        Map<String, WorkerAndTaskList> allocateMap = new HashMap<>();
//        WorkerAndTaskList allocateList;
//        LinkedList<String> cachedTaskList = new LinkedList<>();
//        for (TaskAndWorker tw : taskAndWorkerList) {
//            if (!(tw.worker.length() == 0)) {
//                allocateList = allocateMap.get(tw.worker);
//                if (null == allocateList) {
//                    allocateList = new WorkerAndTaskList(tw.worker);
//                    allocateMap.put(tw.worker, allocateList);
//                }
//                allocateList.taskList.add(tw.task);
//            } else {
//                cachedTaskList.add(tw.task);
//            }
//        }
//        Collections.sort(workerList);
//        int count = numOfMoreTaskWorkers;
//
//        for (String worker : workerList) {
//            allocateList = allocateMap.get(worker);
//            if (null == allocateList) {
//                allocateList = new WorkerAndTaskList(worker);
//                allocateMap.put(worker, allocateList);
//            }
//            if (allocateList.taskList.size() > maxTaskPerWorker) {
//                Collections.sort(allocateList.taskList);
//                int numToRemoved;
//                if (count > 0) {
//                    numToRemoved = allocateList.taskList.size() - maxTaskPerWorker;
//                    --count;
//                } else {
//                    numToRemoved = allocateList.taskList.size() - minTaskPerWorker;
//                }
//                for (; numToRemoved > 0; --numToRemoved) {
//                    cachedTaskList.add(allocateList.taskList.remove());
//                }
//            } else if (allocateList.taskList.size() == maxTaskPerWorker) {
//                if (count > 0) {
//                    --count;
//                } else {
//                    Collections.sort(allocateList.taskList);
//                    cachedTaskList.add(allocateList.taskList.remove());
//                }
//            }
//        }
//        System.out.print("cached task:");
//        for (String cached : cachedTaskList) {
//            System.out.print(cached + ",");
//        }
//        System.out.println();
//        for (String worker : workerList) {
//            allocateList = allocateMap.get(worker);
//            if (allocateList.taskList.size() == minTaskPerWorker) {
//                if (count > 0) {
//                    --count;
//                    allocateList.taskList.add(cachedTaskList.removeFirst());
//                }
//            } else if (allocateList.taskList.size() < minTaskPerWorker) {
//                int numToAdded;
//                if (count > 0) {
//                    numToAdded = maxTaskPerWorker - allocateList.taskList.size();
//                    --count;
//                } else {
//                    numToAdded = minTaskPerWorker - allocateList.taskList.size();
//                }
//                for (; numToAdded > 0; numToAdded--) {
//                    allocateList.taskList.add(cachedTaskList.removeFirst());
//                }
//            }
//        }
//        List<String> myTaskList = allocateMap.get(myId).taskList;
//        Collections.sort(myTaskList);
//        for (String worker : workerList) {
//            System.out.print("Calculated task for " + worker + ":");
//            for (String task : myTaskList) {
//                System.out.print(task + ",");
//            }
//            System.out.println();
//        }
//        return myTaskList;  //To change body of implemented methods use File | Settings | File Templates.
//    }
}
