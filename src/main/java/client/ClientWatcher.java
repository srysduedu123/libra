package client;

import common.exception.ConfigException;
import common.entity.TaskAndWorker;
import common.exception.ExecutorException;
import common.exception.OperationOutOfDateException;
import common.exception.ZKOperationFailedException;
import common.util.LibraZKClient;
import common.util.LibraZKPathUtil;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 *
 * @author :SDU.xccui
 * @version 1.0.0
 *          Date: 13-9-16
 *          Time: 下午10:09
 */
public class ClientWatcher implements Watcher {
    private static Logger LOG = LoggerFactory.getLogger(ClientWatcher.class);
    private String myId;
    private String myAllWorkerPath;

    private ExecutorFactory executorFactory;

    private long projectVersion;
    private ProjectStat projectStat;

    private long workerVersion;
    private List<String> taskWorkerList;

    private long taskAndWorkerVersion;
    private List<TaskAndWorker> taskAndWorkerPairList;

    private LibraZKClient zkClient;
    private IRebalancer rebalancer;

    private Object rebalanceLock;

    private Object zkTaskLock;

    private String hosts;
    private int sessionTimeout;

    public ClientWatcher(String myId, String hosts, int sessionTimeout) throws IOException {
        this.myId = myId;
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
        rebalanceLock = new Object();
        zkTaskLock = new Object();
        myAllWorkerPath = LibraZKPathUtil.genMyAllWorkerPath(myId);
    }

    public void start() throws KeeperException, InterruptedException, ConfigException, IOException {
        zkClient = new LibraZKClient(hosts, sessionTimeout, this);
        workerVersion = 0;
        projectVersion = 0;
        taskAndWorkerVersion = 0;
        taskWorkerList = new ArrayList<String>(0);
        taskAndWorkerPairList = new ArrayList<TaskAndWorker>(0);
        executorFactory = new ExecutorFactory();
        rebalancer = new DefaultRebalancer();
        register();
    }

    public void register() throws KeeperException, InterruptedException, ConfigException {
        zkClient.checkAndCreateNode(LibraZKPathUtil.ALL_WORKER_ROOT, null, CreateMode.PERSISTENT);
        if (zkClient.checkAndCreateNode(myAllWorkerPath, new byte[0], CreateMode.EPHEMERAL)) {//myId node already exists
            throw new ConfigException("myId node " + myId + " already exists at zookeeper");
        }
        //init project stat
        projectStat = ProjectStat.createStandbyState();
        //add watcher to my node
        zkClient.getDataString(myAllWorkerPath, null);
    }

    private void onTaskWorkerChanged() throws KeeperException, InterruptedException {
        LOG.info("onTaskWorkerChanged start");
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (retry > 0) {
            try {
                if (projectStat.isActive()) {
                    zkClient.checkAndCreateNode(projectStat.getMyActiveWorkerRoot(), new byte[0], CreateMode.PERSISTENT);
                    List<String> tempTaskWorkerList = Collections.unmodifiableList(zkClient.getChildren(projectStat.getMyActiveWorkerRoot()));
                    //compare newTaskWorkerList and oldTaskWorkerList
                    if (tempTaskWorkerList.size() != taskWorkerList.size()) {
                        ++workerVersion;
                        taskWorkerList = tempTaskWorkerList;
                    } else {
                        Set<String> currentWorkerSet = new HashSet<String>();
                        currentWorkerSet.addAll(taskWorkerList);
                        Set<String> newWorkerSet = new HashSet<String>();
                        newWorkerSet.addAll(tempTaskWorkerList);
                        if (!currentWorkerSet.containsAll(newWorkerSet)) {
                            ++workerVersion;
                            taskWorkerList = tempTaskWorkerList;
                        }
                    }
                    LOG.info("WorkerVersion=" + workerVersion);
                    System.out.print("Current workers: ");
                    for (String s : taskWorkerList) {
                        System.out.print(s + ", ");
                    }
                    System.out.println();
                }
                break;
            } catch (InterruptedException | KeeperException ex) {
                ex.printStackTrace();
                --retry;
                int retryInterval = WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY);
                ex.printStackTrace();
                LOG.warn("Error reading active workers! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                if (retry <= 0) {
                    throw ex;
                }
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("onTaskWorkerChanged end");
    }

    private String onProjectNameChanged() throws KeeperException, InterruptedException {
        LOG.info("onProjectNameChanged start");
        String newProjectName = projectStat.getMyProjectName();
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (retry > 0) {
            try {
                newProjectName = zkClient.getDataString(myAllWorkerPath, null);
                if (projectStat.isNewProjectName(newProjectName) && newProjectName.length() > 0) {
                    ++projectVersion;
                    LOG.info("ProjectVersion=" + projectVersion);
                    projectStat = ProjectStat.createActiveState(newProjectName);
                } else if (newProjectName.length() == 0) {
                    ++projectVersion;
                    LOG.info("ProjectVersion=" + projectVersion);
                    projectStat = ProjectStat.createStandbyState();
                }
                break;
            } catch (InterruptedException | KeeperException ex) {
                ex.printStackTrace();
                --retry;
                int retryInterval = WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY);
                ex.printStackTrace();
                LOG.warn("Error reading project name! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                if (retry <= 0) {
                    throw ex;
                }
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("onProjectNameChanged end");
        return newProjectName;
    }

    private void onTaskChanged() throws KeeperException, InterruptedException {
        LOG.info("onTaskChanged start");
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (true) {
            try {
                if (projectStat.isActive()) {
                    String projectName = projectStat.getMyProjectName();
                    zkClient.checkAndCreateNode(projectStat.getMyTaskRoot(), new byte[0], CreateMode.PERSISTENT);
                    List<String> taskList = zkClient.getChildren(projectStat.getMyTaskRoot());
                    List<TaskAndWorker> tempTaskAndWorkerList = new LinkedList<TaskAndWorker>();
                    String workerForTask;
                    for (String taskName : taskList) {
                        try {
                            workerForTask = zkClient.checkAndGetDataString(LibraZKPathUtil.genSingleTaskPath(taskName, projectName));
                            tempTaskAndWorkerList.add(new TaskAndWorker(taskName, workerForTask));
                        } catch (InterruptedException | KeeperException ex) {
                            //TODO how to deal with?
                        }
                    }
                    ++taskAndWorkerVersion;
                    taskAndWorkerPairList = Collections.unmodifiableList(tempTaskAndWorkerList);
                }
                break;
            } catch (InterruptedException | KeeperException ex) {
                ex.printStackTrace();
                --retry;
                int retryInterval = WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY);
                ex.printStackTrace();
                LOG.warn("Error reading tasks! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                if (retry <= 0) {
                    throw ex;
                }
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("onTaskChanged end");
    }

    private void checkState(long projectVersion, long taskAndWorkerVersion, long workerVersion) throws OperationOutOfDateException {
        if (!(projectVersion == this.projectVersion && taskAndWorkerVersion == this.taskAndWorkerVersion && workerVersion == workerVersion)) {
            throw new OperationOutOfDateException("State is out of date");
        }
    }

    private void checkAndOwnTasks(long projectVersion, long taskAndWorkerVersion, long workerVersion, String projectName, List<String> myTasks, List<String> ownedTasksContainer) throws OperationOutOfDateException, ZKOperationFailedException {
        synchronized (zkTaskLock) {
            LOG.info("checkAndOwnTasks");
            checkState(projectVersion, taskAndWorkerVersion, workerVersion);
            String taskOwner;
            LinkedList<String> taskToBeOwned = new LinkedList<String>();
            for (String myTask : myTasks) {
                taskToBeOwned.add(myTask);
            }
            String myTask;
            Set<String> workerSet = new HashSet<String>();
            while (taskToBeOwned.size() > 0) {
                workerSet.clear();
                workerSet.addAll(taskWorkerList);
                myTask = taskToBeOwned.removeFirst();
                try {
                    taskOwner = zkClient.checkAndGetDataString(LibraZKPathUtil.genSingleTaskPath(myTask, projectName));
                    if (taskOwner.length() > 0 && workerSet.contains(taskOwner) && !taskOwner.equals(myId)) {//not released yet
                        taskToBeOwned.addLast(myTask);
                    } else {//can be owned
                        zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(myTask, projectName), taskOwner, myId.getBytes(), false);
                        ownedTasksContainer.add(myTask);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                    checkState(projectVersion, taskAndWorkerVersion, workerVersion);
                    LOG.warn("Can not own task:" + myTask + " will retry in " + WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY) + "ms");
                    try {
                        Thread.sleep(WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY));
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
                checkState(projectVersion, taskAndWorkerVersion, workerVersion);
            }
        }
    }

    /**
     * @param projectName
     * @param ownedTaskList
     * @param taskVersion   if taskVersion == -1,it will not be checked
     * @throws ZKOperationFailedException
     * @throws OperationOutOfDateException if taskVersion >=0 and is smaller than current taskVersion
     */
    private void forceReleaseOwnedTasks(String projectName, List<String> ownedTaskList, long taskVersion) throws ZKOperationFailedException, OperationOutOfDateException {
        synchronized (zkTaskLock) {
            if (taskVersion >= 0 && taskVersion < executorFactory.getCurrentTaskVersion()) {
                throw new OperationOutOfDateException("task version:" + taskVersion + " is out of date, current is " + executorFactory.getCurrentTaskVersion());
            }
            LOG.info("forceReleaseOwnedTasks");
            int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
            LinkedList<String> ownedTasks = new LinkedList<String>();
            ownedTasks.addAll(ownedTaskList);
            String ownedTask;
            while (ownedTasks.size() > 0) {
                ownedTask = ownedTasks.removeFirst();
                try {
                    zkClient.compareAndUpdateData(LibraZKPathUtil.genSingleTaskPath(ownedTask, projectName), myId, new byte[0], false);
                } catch (KeeperException | InterruptedException ex) {
                    if (ex instanceof KeeperException && ((KeeperException) ex).code() == KeeperException.Code.NONODE) {
                        //the task may be removed by server
                        continue;
                    }
                    ownedTasks.addLast(ownedTask);
                    ex.printStackTrace();
                    --retry;
                    if (retry <= 0) {
                        throw new ZKOperationFailedException("Can not release my task:" + ownedTask);
                    }
                    LOG.warn("Can not release my task:" + ownedTask + " will retry in " + WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY) + "ms, " + retry + " times left.");
                    try {
                        Thread.sleep(WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (taskVersion >= 0 && taskVersion < executorFactory.getCurrentTaskVersion()) {
                throw new OperationOutOfDateException("task version:" + taskVersion + " is out of date, current is " + executorFactory.getCurrentTaskVersion());
            }
        }
    }

    private void addMyIdToActiveWorkerRoot(String newProjectName) throws ZKOperationFailedException {
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (true) {
            try {
                //add myId to new active worker root
                zkClient.checkAndCreateNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, newProjectName), new byte[0], CreateMode.EPHEMERAL);
                break;
            } catch (KeeperException | InterruptedException ex) {
                ex.printStackTrace();
                --retry;
                if (retry <= 0) {
                    throw new ZKOperationFailedException("Can not create my active node for project:" + newProjectName);
                }
                LOG.warn("Can not create my active node for project:" + newProjectName + "will retry in " + WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY) + "ms, " + retry + " times left.");
                try {
                    Thread.sleep(WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void deleteMyIdFromActiveWorkerRoot(String oldProjectName) throws ZKOperationFailedException {
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (true) {
            try {
                zkClient.checkAndDeleteNode(LibraZKPathUtil.genMyActiveWorkerPath(myId, oldProjectName));
                break;
            } catch (KeeperException | InterruptedException ex) {
                ex.printStackTrace();
                --retry;
                if (retry <= 0) {
                    throw new ZKOperationFailedException("Can not delete my active node for project:" + oldProjectName);
                }
                LOG.warn("Can not delete my active node for project:" + oldProjectName + "will retry in " + WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY) + "ms, " + retry + " times left.");
                try {
                    Thread.sleep(WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dealWithTaskOrWorkerChanged(final String projectName) {
        LOG.info("DealWithTaskOrWorkerChanged");
        final long projectVersionSnapshot = projectVersion;
        final long workerVersionSnapshot = workerVersion;
        final long taskAndWorkerVersionSnapshot = taskAndWorkerVersion;
        final List<String> oldTaskList = executorFactory.getCurrentTaskList();
        final long taskVersion = executorFactory.getCurrentTaskVersion();
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (rebalanceLock) {
                    LinkedList<String> ownedTaskList = new LinkedList<String>();
                    try {
                        executorFactory.pauseCurrentExecutor(projectName);
                        try {
                            forceReleaseOwnedTasks(projectName, oldTaskList, taskVersion);
                        } catch (OperationOutOfDateException ex) {
                            ex.printStackTrace();
                            return;
                        }
                        checkState(projectVersionSnapshot, taskAndWorkerVersionSnapshot, workerVersionSnapshot);
                        List<String> taskList = rebalancer.calculateMyTask(myId, taskAndWorkerPairList, taskWorkerList);
                        checkState(projectVersionSnapshot, taskAndWorkerVersionSnapshot, workerVersionSnapshot);
                        checkAndOwnTasks(projectVersionSnapshot, taskAndWorkerVersionSnapshot, workerVersionSnapshot, projectName, taskList, ownedTaskList);
                        executorFactory.startCurrentExecutor(projectName, taskList);
                    } catch (OperationOutOfDateException | ExecutorException e) {
                        e.printStackTrace();
                        try {
                            forceReleaseOwnedTasks(projectName, ownedTaskList, -1);
                        } catch (ZKOperationFailedException | OperationOutOfDateException ex) {
                            forceExit();
                        }
                    } catch (ZKOperationFailedException e) {
                        e.printStackTrace();
                        try {
                            forceReleaseOwnedTasks(projectName, ownedTaskList, -1);
                        } catch (ZKOperationFailedException | OperationOutOfDateException ex) {
                            ex.printStackTrace();
                        } finally {
                            forceExit();
                        }
                    }
                }
            }
        }).start();
    }

    private void activate(final String projectName) {
        LOG.info("Activate - projectName:" + projectName);
        try {
            addMyIdToActiveWorkerRoot(projectName);
            executorFactory.loadExecutor(projectName);
        } catch (ZKOperationFailedException | ExecutorException e) {
            e.printStackTrace();
            forceExit();
        }
    }

    private void reactivate(final String newProjectName, final String oldProjectName) {
        LOG.info("Reactivate - projectName:" + newProjectName);
        //stop current executor and release tasks on zk
        try {
            executorFactory.stopCurrentExecutor();
            forceReleaseOwnedTasks(oldProjectName, executorFactory.getCurrentTaskList(), -1);
            deleteMyIdFromActiveWorkerRoot(oldProjectName);
            addMyIdToActiveWorkerRoot(newProjectName);
            executorFactory.loadExecutor(newProjectName);
        } catch (ExecutorException | ZKOperationFailedException | OperationOutOfDateException ex) {
            ex.printStackTrace();
            forceExit();
        }
    }

    private void inactivate(final String oldProjectName) {
        LOG.info("Inactivate - projectName:" + oldProjectName);
        try {
            executorFactory.stopCurrentExecutor();
            deleteMyIdFromActiveWorkerRoot(oldProjectName);
            forceReleaseOwnedTasks(oldProjectName, executorFactory.getCurrentTaskList(), -1);
        } catch (ExecutorException | ZKOperationFailedException | OperationOutOfDateException ex) {
            ex.printStackTrace();
            forceExit();
        }
    }

    private void forceExit() {
        LOG.error("Fatal error! Will kill myself to release.");
        try {
            executorFactory.stopCurrentExecutor();
            zkClient.close();
        } catch (ExecutorException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.exit(-1);
        }
    }

    private void resetUnsupportedProjectName(String newProjectName, String oldProjectName) throws ZKOperationFailedException {
        int retry = WorkerConfig.getIntProperty(WorkerConfig.RETRY_TIMES_KEY);
        while (retry > 0) {
            try {
               zkClient.compareAndUpdateData(myAllWorkerPath, newProjectName, oldProjectName.getBytes(), false);
                break;	//return loop? fixed bug
            } catch (KeeperException | InterruptedException ex) {
                ex.printStackTrace();
                LOG.debug("Exception:" + retry);
                --retry;
                if (retry <= 0) {
                    throw new ZKOperationFailedException("Can not reset my task from " + newProjectName + " to " + oldProjectName);
                }
                LOG.warn("Can not reset my task from " + newProjectName + " to " + oldProjectName + "will retry in " + WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY) + "ms, " + retry + " times left.");
                try {
                    Thread.sleep(WorkerConfig.getIntProperty(WorkerConfig.RETRY_INTERVAL_KEY));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("zkWatcher process:" + watchedEvent.getState() + "\t" + watchedEvent.getPath() + "\t" + watchedEvent.getType());
        if (watchedEvent.getState().equals(Event.KeeperState.Disconnected)) {
            LOG.warn("Zookeeper disconnected! Will try to reconnect!");
            try {
                zkClient.close();
                executorFactory.stopCurrentExecutor();
            } catch (ExecutorException | InterruptedException e) {
                e.printStackTrace();
            }
            try {
                start();
            } catch (Exception ex) {
                forceExit();
            }
        } else {
            try {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    if (watchedEvent.getPath().contains(projectStat.getMyActiveWorkerRoot())) {
                        final long oldWorkerVersion = workerVersion;
                        onTaskWorkerChanged();
                        if (oldWorkerVersion < workerVersion) {// worker changed
                            LOG.info("worker changed");
                            dealWithTaskOrWorkerChanged(projectStat.getMyProjectName());
                        }
                    } else if (watchedEvent.getPath().contains(projectStat.getMyTaskRoot())) {
                        final long oldTaskAndWorkerVersion = taskAndWorkerVersion;
                        onTaskChanged();
                        if (oldTaskAndWorkerVersion < taskAndWorkerVersion) {// task changed
                            LOG.info("task changed");
                            dealWithTaskOrWorkerChanged(projectStat.getMyProjectName());
                        }
                    }
                } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged && watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {
                    final long oldProjectVersion = projectVersion;
                    final ProjectStat oldProjectStat = projectStat;
                    final String newProjectName = onProjectNameChanged();
                    if (oldProjectVersion < projectVersion) {// project changed
                        if (executorFactory.isProjectSupported(newProjectName)) {
                            if (oldProjectStat.isActive()) {//reactivate
                                if (projectStat.isActive()) {
                                    LOG.info("from active to active");
                                    reactivate(newProjectName, oldProjectStat.getMyProjectName());
                                    onTaskWorkerChanged();
                                    onTaskChanged();
                                    dealWithTaskOrWorkerChanged(newProjectName);
                                } else {//inactivate
                                    LOG.info("from active to inactive");
                                    inactivate(oldProjectStat.getMyProjectName());
                                }
                            } else {//activate
                                if (projectStat.isActive()) {
                                    LOG.info("from inactive to active");
                                    activate(newProjectName);
                                    onTaskWorkerChanged();
                                    onTaskChanged();
                                    dealWithTaskOrWorkerChanged(newProjectName);
                                }
                            }
                        } else {
                            //not supported projectName
                            LOG.info("Not supported projectName " + newProjectName + ", will reset to " + oldProjectStat.getMyProjectName());
                            --projectVersion;
                            projectStat = oldProjectStat;
                            try {
                                resetUnsupportedProjectName(newProjectName, oldProjectStat.getMyProjectName());
                            } catch (ZKOperationFailedException e) {
                                e.printStackTrace();
                                forceExit();
                            }
                        }
                    }else{
                    	LOG.info("project version equal");
                    }
                }else if (watchedEvent.getType() == Event.EventType.NodeDeleted&& watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {	
                	//delete from all worker list
					try {
						forceReleaseOwnedTasks(projectStat.getMyProjectName(), executorFactory.getCurrentTaskList(), -1);
						deleteMyIdFromActiveWorkerRoot(projectStat.getMyProjectName());
					} catch (ZKOperationFailedException
							| OperationOutOfDateException e) {
						e.printStackTrace();
					}
				}
            } catch (KeeperException | InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    private static void showUsage(){
		System.out.println("Usage: -n workername [-t timeout(default:10000)]|[-h host (defalut:localhost)]");
		System.exit(1);
    }
    
    public static void main(String args[]) {
	        try {
	    		String workerName = null;
	    		String host = "localhost:2181";
	    		int timeout = 10000;
	    		if(args.length == 2){
	    			if("-n".equals(args[0])){
	    				workerName = args[1];
	    			}else{
	    				showUsage();
	    			}
	    		}else if(args.length == 4){
	    			if("-n".equals(args[0]) && "-t".equals(args[2])){
	    				workerName = args[1];
	    				timeout = Integer.valueOf(args[3]);
	    			}else if("-n".equals(args[0]) && "-h".equals(args[2])){
	    				workerName = args[1];
	    				host = args[3];
	    			}else{
	    				showUsage();
	    			}
	    		}else if(args.length == 6){
	    			
	    			if("-n".equals(args[0]) && "-t".equals(args[2]) && "-h".equals(args[4])){
	    				workerName = args[1];
	    				timeout = Integer.valueOf(args[3]);
	    				host = args[5];
	    			}else{
	    				showUsage();
	    			}
	    			
	    		}else{
	    			showUsage();
	    		}
	        	ClientWatcher watcher = new ClientWatcher(workerName, host , timeout);
				watcher.start();
				System.out.println("INFO: Worker " + workerName + " at " + host +" is Running... ");
				Thread.sleep(6000000);
				System.out.println("INFO: Worker " + workerName + "exit.");
	    } catch (IOException e) {
	        e.printStackTrace();
	    }  catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
