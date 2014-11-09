package cn.edu.sdu.cs.starry.libra.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author  xccui
 * Date: 13-10-12
 * Time: 9:17
 */
public class DefaultRebalanceTool implements IRebalanceTool {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRebalanceTool.class);

    @Override
    public List<String> calculateMyTask(String myId, List<String> immutableTaskList, List<String> immutableWorkerList){
        if (immutableWorkerList.size() == 0) {
            return new ArrayList<String>();
        }
        ArrayList<String> workerList = new ArrayList<String>(immutableWorkerList.size());
        workerList.addAll(immutableWorkerList);
        Collections.sort(workerList);
        int myOrder = workerList.indexOf(myId);
        int workerNum = workerList.size();
        List<String> myTaskList = new LinkedList<String>();
        for (int i = 0; i < immutableTaskList.size(); i++) {
            if (i % workerNum == myOrder) {
                myTaskList.add(immutableTaskList.get(i));
            }
        }
        LOG.info("Calculated my tasks: " + myTaskList);
        return myTaskList;
    }


    private static class WorkerAndTaskList {
        String worker;
        LinkedList<String> taskList = new LinkedList<String>();

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
