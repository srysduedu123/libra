package client;

import common.exception.ConfigException;
import common.exception.ZKOperationFailedException;
import common.util.LibraZKClient;
import common.util.LibraZKPathUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author xccui
 * Date: 14-10-20
 * Time: 9:26
 */
public class LibraClientAgent {
    private static Logger LOG = LoggerFactory.getLogger(LibraClientAgent.class);
    private LibraZKClient zkClient;
    private Object zkTaskLock;
    private String myId;
    private String myAllWorkerPath;

    public LibraClientAgent(Watcher watcher,String myId, String hosts, int sessionTimeout) throws IOException {
        this.myId = myId;
        zkClient = new LibraZKClient(hosts, sessionTimeout, watcher);
        zkTaskLock = new Object();
        myAllWorkerPath  = LibraZKPathUtil.genMyAllWorkerPath(myId);
    }

    /**
     * Register client with myId to ZK
     *
     * @throws KeeperException
     * @throws InterruptedException
     * @throws ConfigException
     */
    void register() throws KeeperException, InterruptedException, ConfigException {
        zkClient.checkAndCreateNode(LibraZKPathUtil.ALL_WORKER_ROOT, null, CreateMode.PERSISTENT);
        if (zkClient.checkAndCreateNode(myAllWorkerPath, new byte[0], CreateMode.EPHEMERAL)) {//myId node already exists
            throw new ConfigException("id node " + myId + " already exists at zookeeper");
        }
        //add watcher to my node
        zkClient.getDataString(myAllWorkerPath, null);
    }

    List<String> fetchWorkerList(String currentWorkerRoot) throws KeeperException, InterruptedException {
            zkClient.checkAndCreateNode(currentWorkerRoot, new byte[0], CreateMode.PERSISTENT);
            return Collections.unmodifiableList(zkClient.getChildren(currentWorkerRoot));
    }

    List<String> fetchTaskList(String currentTaskRoot) throws KeeperException, InterruptedException {
            zkClient.checkAndCreateNode(currentTaskRoot, new byte[0], CreateMode.PERSISTENT);
            return Collections.unmodifiableList(zkClient.getChildren(currentTaskRoot));
    }

    String fetchMyProjectName() throws KeeperException, InterruptedException {
            return zkClient.getDataString(myAllWorkerPath, null);
    }


    void checkAndOwnTasks(String projectName, Set<String> myTasks, List<String> workerList, Set<String> ownedTasks) throws KeeperException, InterruptedException, ZKOperationFailedException {
        synchronized (zkTaskLock) {
            LOG.info("checkAndOwnTasks");
            String taskOwner;
            Set<String> workerSet = new HashSet<>(workerList);
            for(String myTask : myTasks){
                taskOwner = zkClient.checkAndGetDataString(LibraZKPathUtil.genSingleTaskPath(myTask, projectName));
                if (taskOwner.length() > 0 && workerSet.contains(taskOwner) && !taskOwner.equals(myId)) {
                    //not released yet
                } else {
                    //can be owned
                    zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(myTask, projectName), taskOwner, myId.getBytes(), false);
                    ownedTasks.add(myTask);
                }
            }
            LOG.info("Owned tasks: " + ownedTasks);
            if( myTasks.size() != ownedTasks.size()) {
                myTasks.removeAll(ownedTasks);
                throw new ZKOperationFailedException("Task " + myTasks + " are not owned");
            }
        }
    }

    /**
     * Release my owned tasks from ZK
     *
     * @param projectName
     * @param taskToBeReleased
     */
    void releaseTasks(String projectName, Set<String> taskToBeReleased, Set<String> taskReleased) throws KeeperException, InterruptedException, ZKOperationFailedException {
        synchronized (zkTaskLock) {
            LOG.info("releaseTasks");
            LinkedList<String> ownedTasks = new LinkedList<>(taskToBeReleased);
            for(String task : taskToBeReleased){
                task = ownedTasks.removeFirst();
                if(zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(task, projectName), myId, new byte[0], false)) {
                    taskReleased.add(task);
                }
            }
            LOG.info("Released tasks: " + taskReleased);
            if(taskReleased.size() != taskReleased.size()) {
                taskToBeReleased.removeAll(taskReleased);
                throw new ZKOperationFailedException("Task " + taskToBeReleased + " are not released");
            }
        }
    }

    void addMyIdToActiveWorkerRoot(String newProjectName) throws KeeperException, InterruptedException {
        zkClient.checkAndCreateNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, newProjectName), new byte[0], CreateMode.EPHEMERAL);
    }

    void deleteMyIdFromActiveWorkerRoot(String oldProjectName) throws KeeperException, InterruptedException {
        zkClient.checkAndDeleteNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, oldProjectName));
    }

    void resetUnsupportedProjectName(String newProjectName, String oldProjectName) throws KeeperException, InterruptedException {
        zkClient.compareAndUpdateData(myAllWorkerPath, newProjectName, oldProjectName.getBytes(), false);
    }


    public void close() throws InterruptedException {
        if (null != zkClient) {
            zkClient.close();
        }
    }

    public void reset() {
        zkClient.reconnect();
    }
}
