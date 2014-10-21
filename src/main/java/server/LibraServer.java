package server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.util.LibraZKClient;
import common.util.LibraZKPathUtil;

/**
 * Server ZK watcher for client stats and project operations
 * @author xccui
 */
public class LibraServer implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(LibraServer.class);
    private LibraZKClient zkClient;

    public LibraServer(String host, int sessionTimeout) throws IOException {
        zkClient = new LibraZKClient(host, sessionTimeout, this);
        LOG.info("zkClient init ");
    }

    /**
     * @return maybe null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getAllProjects() throws KeeperException, InterruptedException {
        if (!zkClient.checkNodeExist(LibraZKPathUtil.PROJECT_ROOT)) {
            zkClient.checkAndCreateNode(LibraZKPathUtil.PROJECT_ROOT, null, CreateMode.PERSISTENT);
        }
        return zkClient.getChildren(LibraZKPathUtil.PROJECT_ROOT);
    }

    /**
     * @return maybe null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getAllWorkers() throws KeeperException, InterruptedException {
        if (!zkClient.checkNodeExist(LibraZKPathUtil.ALL_WORKER_ROOT)) {
            zkClient.checkAndCreateNode(LibraZKPathUtil.ALL_WORKER_ROOT, null, CreateMode.PERSISTENT);
        }
        return zkClient.getChildren(LibraZKPathUtil.ALL_WORKER_ROOT);
    }

    /**
     * Get task list for given project name
     * @param projectName
     * @return null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getProjectTasks(String projectName) throws KeeperException, InterruptedException {
        List<String> tasks = null;
        try {
            tasks = zkClient.getChildren(LibraZKPathUtil.genTaskRootPath(projectName));
        } catch (KeeperException.NoNodeException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
        return tasks;
    }

    /**
     * @param projectName
     * @return maybe null
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getProjectWorkers(String projectName) throws KeeperException, InterruptedException {
        List<String> workers = null;
        List<String> allWorkers = null;
        List<String> activeWorkers = new LinkedList<String>();
        try {
            allWorkers = getAllWorkers();
            workers = zkClient.getChildren(LibraZKPathUtil.genActiveWorkerRootPath(projectName));
            if (workers != null) {
                if (allWorkers != null) {
                    for (String worker : allWorkers) {
                        if (workers.contains(worker)) {
                            if (projectName.equals(getProjectNameForWorker(worker))) {
                                activeWorkers.add(worker);
                            } else {
                                LOG.info("conflict: worker still exist in project,but worker's nodedata clear.");
                                //zkClient.checkAndDeleteNode(LibraZKPathUtil.genMyActiveWorkerPath(worker, projectName));
                                //if delete would be error,when worker adding, this path isn't exist,but will be created later.

                            }
                        }
                    }
                }
                return activeWorkers;
            }

        } catch (KeeperException.NoNodeException e) {

        }
        return workers;
    }

    /**
     * @param workerId
     * @return List<String> if no tasks List's size should be zero.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getWorkerTasks(String workerId) throws KeeperException, InterruptedException {
        String projectName = zkClient.checkAndGetDataString(LibraZKPathUtil
                .genMyAllWorkerPath(workerId));
        List<String> taskList = new ArrayList<String>();
        if (projectName != null && !projectName.equals("")) {
            List<String> tasks = getProjectTasks(projectName);
            if (tasks != null) {
                for (String task : tasks) {
                    String worker = zkClient.checkAndGetDataString(LibraZKPathUtil
                            .genSingleTaskPath(task, projectName));
                    if (workerId.equals(worker)) {
                        taskList.add(task);
                    }
                }
            }
        }
        return taskList;

    }

    /**
     * Get
     *
     * @param workerName
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    public String getProjectNameForWorker(String workerName) throws KeeperException, InterruptedException {
        return zkClient.checkAndGetDataString(LibraZKPathUtil.genMyAllWorkerPath(workerName));
    }

    /**
     * @param projectName
     * @return true: already exist,add failed.	false: add success.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean addProject(String projectName) throws KeeperException, InterruptedException {
        return zkClient.checkAndCreateNode(LibraZKPathUtil.PROJECT_ROOT + "/"
                + projectName, projectName.getBytes(), CreateMode.PERSISTENT);
    }

    /**
     * Remove the given project
     *
     * @param projectName
     * @return true
     *         if remove success
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean removeProject(String projectName) throws KeeperException, InterruptedException {
        boolean result = true;
        List<String> pathList = zkClient.getChildren(LibraZKPathUtil.PROJECT_ROOT + "/" + projectName);
        for (String path : pathList) {
            if (path.endsWith("tasks")) {
                List<String> tasks = getProjectTasks(projectName);
                if (tasks != null) {
                    for (String task : tasks) {
                        result &= deleteTask(projectName, task);
                    }
                }
                zkClient.checkAndDeleteNode(LibraZKPathUtil.genTaskRootPath(projectName));
            } else if (path.endsWith("workers")) {
                List<String> workers = getProjectWorkers(projectName);
                if (workers != null) {
                    for (String worker : workers) {
                        if (projectName.equals(getProjectNameForWorker(worker))) {
                            result &= evacuateWorker(worker, projectName);
                        } else {
                            zkClient.checkAndDeleteNode(LibraZKPathUtil.genMyActiveWorkerPath(worker, projectName));
                        }
                    }
                }
                zkClient.checkAndDeleteNode(LibraZKPathUtil.genActiveWorkerRootPath(projectName));
            }
        }
        return result & zkClient.checkAndDeleteNode(LibraZKPathUtil.PROJECT_ROOT + "/" + projectName);
    }

    /**
     * @param projectName
     * @param taskName
     * @return true : add task failed
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean addTask(String projectName, String taskName) throws KeeperException, InterruptedException {
        return zkClient.checkAndCreateNode(LibraZKPathUtil.genSingleTaskPath(taskName, projectName),
                new byte[0], CreateMode.PERSISTENT);
    }

    /**
     * @param projectName
     * @param taskName
     * @return true: delete success
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean deleteTask(String projectName, String taskName)
            throws KeeperException, InterruptedException {
        return zkClient.checkAndDeleteNode(LibraZKPathUtil.genSingleTaskPath(taskName, projectName));
    }

    /**
     * @param workerId
     * @param projectName
     * @return true : assign worker success
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public boolean assignWorker(String workerId, String projectName)
            throws KeeperException, InterruptedException, IOException {
        return zkClient.setDataString(LibraZKPathUtil
                .genMyAllWorkerPath(workerId), projectName);
    }

    /**
     * @param workerId
     * @param projectName
     * @return true : evacuate worker success
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean evacuateWorker(String workerId, String projectName)
            throws KeeperException, InterruptedException {
        return zkClient.setDataString(LibraZKPathUtil.genMyAllWorkerPath(workerId), "");
    }

    @Override
    public void process(WatchedEvent arg0) {


    }

    /**
     * get worker startup time
     *
     * @param workerName
     * @return if 0 ,false
     * @throws KeeperException
     * @throws InterruptedException
     */
    public long getWorkerTime(String workerName) throws KeeperException, InterruptedException {
        if (zkClient.checkNodeExist(LibraZKPathUtil.genMyAllWorkerPath(workerName))) {
            return (new Date().getTime() - zkClient.getNodeCreateTime(LibraZKPathUtil.genMyAllWorkerPath(workerName)));
        } else
            return 0;

    }

    /**
     * get worker add to project time
     *
     * @param projectName
     * @param workerName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public long getProjectWorkerTime(String projectName, String workerName) throws KeeperException, InterruptedException {
        String projectWorkerPath = LibraZKPathUtil.genMyActiveWorkerPath(workerName, projectName);
        if (zkClient.checkNodeExist(projectWorkerPath)) {
            return (new Date().getTime() - zkClient.getNodeCreateTime(projectWorkerPath));
        } else {
            return 0;
        }

    }

    public void printStatus() throws KeeperException, InterruptedException {
        LOG.info("=============== PROJECTS LIST  ==================");
        List<String> projects = getAllProjects();
        if (projects != null) {
            for (String project : projects) {
                LOG.info("ProjectName:" + project);
                List<String> tasks = getProjectTasks(project);
                if (tasks != null) {
                    for (String task : tasks) {
                        LOG.info("TaskName:" + task);
                    }
                } else {
                    LOG.info("EMPTY TASK");
                }
                List<String> workers = getProjectWorkers(project);
                if (workers != null && workers.size() != 0) {
                    for (String worker : workers) {
                        LOG.info("WorkerName:" + worker);
                        LOG.info("ProWorkerTime:" + getProjectWorkerTime(project, worker));
                    }
                } else {
                    LOG.info("%%% EMPTY WORKER %%%");
                }
            }
        } else {
            LOG.info(" ### EMPTY PROJECT ### ");
        }

        LOG.info("=============== WORKER LIST  ==================");
        List<String> workers = getAllWorkers();
        if (workers != null) {
            for (String worker : workers) {
                LOG.info("WorkerName:" + worker);
                LOG.info("RunningTime:" + getWorkerTime(worker));
                List<String> tasks = getWorkerTasks(worker);
                if (tasks.size() != 0) {
                    for (String task : tasks) {
                        LOG.info("@Worker==Task: " + task);
                    }
                } else {
                    LOG.info("@WORKER== NULL TASK");
                }
            }
        } else {
            LOG.info("@@@@@ EMPTY WORKER @@@@@");
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        LibraServer watcher = new LibraServer("192.168.238.10:2181", 10000);
        watcher.printStatus();
    }
}
