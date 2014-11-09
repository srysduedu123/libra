package cn.edu.sdu.cs.starry.libra.client;

import cn.edu.sdu.cs.starry.libra.client.config.LibraClientConfig;
import cn.edu.sdu.cs.starry.libra.client.state.ClientDataVersion;
import cn.edu.sdu.cs.starry.libra.client.state.ProjectState;
import cn.edu.sdu.cs.starry.libra.common.exception.*;
import cn.edu.sdu.cs.starry.libra.common.util.LibraZKPathUtil;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Main class for libra client
 *
 * @author xccui
 * Date: 13-9-16
 * Time: 10:09
 */
public class LibraClient {
    private static Logger LOG = LoggerFactory.getLogger(LibraClient.class);

    String myId;
    String myAllWorkerPath;
    String hosts;
    int sessionTimeout;
    final int retryInterval = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_INTERVAL_KEY);

    public enum LibraClientEvent {
        projectChanged, taskListChanged, workerListChanged
    }
    private ExecutorFactory executorFactory;

    volatile ProjectState projectState;
    private volatile List<String> workerList;
    private volatile List<String> taskList;
    private volatile List<String> myTaskList;
    private volatile ClientDataVersion currentState;

    private Object rebalanceLock;
    private IRebalanceTool rebalanceTool;
    private LibraClientWatcher watcher;
    private LibraClientAgent agent;
    public LibraClient(String myId, String hosts, int sessionTimeout, ExecutorFactory executorFactory){
        this.myId = myId;
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
        this.executorFactory = executorFactory;
        myAllWorkerPath = LibraZKPathUtil.genMyAllWorkerPath(myId);
        watcher = new LibraClientWatcher(this);
        agent = new LibraClientAgent(myId,hosts,sessionTimeout);
        rebalanceTool = new DefaultRebalanceTool();
        rebalanceLock = new Object();
        currentState = new ClientDataVersion();
    }
    void register() throws InterruptedException, ConfigException, KeeperException, IOException {
        agent.register(watcher);
        //init project stat
        projectState = ProjectState.createStandbyState();
    }
    public void start() throws KeeperException, InterruptedException, ConfigException, IOException {
        workerList = new ArrayList<>();
        taskList = new ArrayList<>();
        register();
    }
    public void restart(){
        try {
            agent.close();
            executorFactory.stopCurrentExecutor();
        } catch (ExecutorException | InterruptedException e) {
            e.printStackTrace();
        }
        try {
            start();
        } catch (Exception ex) {
            ex.printStackTrace();
            forceExit();
        }
    }
    private synchronized void activate(final String projectName, ClientDataVersion snapshot) throws KeeperException, InterruptedException, ExecutorException {
        LOG.info("Activate - projectName:" + projectName);
        agent.addMyIdToActiveWorkerRoot(projectName);
        executorFactory.loadExecutor(projectName);
        currentState.updateProjectVersion();
        snapshot.updateProjectVersion();
    }

    private synchronized void inactivate(final String oldProjectName, ClientDataVersion snapshot) throws KeeperException, InterruptedException, WrongStateException {
        LOG.info("Inactivate - projectName:" + oldProjectName);
        executorFactory.stopCurrentExecutor();
        agent.deleteMyIdFromActiveWorkerRoot(oldProjectName);
        currentState.updateProjectVersion();
        snapshot.updateProjectVersion();
    }
    private synchronized void reassignTasks(Set<String> taskToBeReleased, Set<String> taskReleased, Set<String> taskTobeOwned, Set<String> taskOwned) throws OperationOutOfDateException, ExecutorException, KeeperException, InterruptedException, ZKOperationFailedException {
        taskToBeReleased.removeAll(taskReleased);
        taskTobeOwned.removeAll(taskOwned);
        executorFactory.pauseCurrentExecutor(projectState.getProjectName());
        agent.releaseTasks(projectState.getProjectName(),taskToBeReleased, taskReleased);
        agent.checkAndOwnTasks(projectState.getProjectName(),taskTobeOwned, workerList, taskOwned);
        executorFactory.startCurrentExecutor(projectState.getProjectName(), myTaskList);
    }

    public synchronized void handleEvent(LibraClientEvent event) throws InterruptedException {
		LOG.info(event.name() + " start");
        int retry = LibraClientConfig.getIntProperty(LibraClientConfig.RETRY_TIMES_KEY);
        boolean success = false;
        ClientDataVersion snapshot = currentState.makeSnapshot();
        Set<String> taskToBeReleased = new HashSet<>(executorFactory.getCurrentTaskList());
        Set<String> taskToBeOwned = new HashSet<>();
        Set<String> taskReleased = new HashSet<>();
        Set<String> taskOwned = new HashSet<>();
        String newProjectName = null;
        boolean inactive = true;
        boolean active = true;
        boolean recalculate = true;
        boolean updateTaskList = true;
        boolean updateWorkerList = true;
        ProjectState oldProjectState = projectState;
        LOG.info("=============== Handle Event start ===============");
        while (!success && retry > 0) {
            try{
                currentState.checkOutOfDate(snapshot);
                switch (event){
                    case projectChanged:
                        if(null == newProjectName){
                            newProjectName = agent.fetchMyProjectName();
                        }
                        if (oldProjectState.isNewProjectName(newProjectName) && newProjectName.length() > 0) {
                            projectState = ProjectState.createActiveState(newProjectName);
                            if(executorFactory.isProjectSupported(newProjectName)){
                                if(oldProjectState.isActive() && inactive) {
                                    //reactive
                                    LOG.info("from active to inactive");
                                    inactivate(oldProjectState.getProjectName(), snapshot);
                                    inactive = false;
                                }
                                //active
                                if(active){
                                    LOG.info("from inactive to active");
                                    activate(newProjectName, snapshot);
                                    active = false;
                                }
                                if(updateTaskList) {
                                    setTaskList(agent.fetchTaskList(projectState.getCurrentTaskRoot()), snapshot);
                                    //updateTaskList = false;
                                }
                                if(updateWorkerList) {
                                    setWorkerList(agent.fetchWorkerList(projectState.getCurrentWorkerRoot()), snapshot);
                                    //updateWorkerList = false;
                                    LOG.info("workerList:" + workerList);
                                }
                                if(!workerList.contains(myId)){
                                	LOG.info("current workerlist do not contain myId");
                                	forceExit();
                                }
                                if(recalculate) {
                                    recalculateMyTasks();
                                    taskToBeOwned = new HashSet<>();
                                    taskOwned = new HashSet<>();
                                    taskReleased = new HashSet<>();
                                    taskToBeOwned.addAll(myTaskList);
                                    //recalculate = false;
                                }
                                reassignTasks(taskToBeReleased,taskReleased, taskToBeOwned, taskOwned);
                            }else {
                                LOG.info("Not supported projectName " + newProjectName + ", will reset to " + oldProjectState.getProjectName());
                                projectState = oldProjectState;
                                agent.resetUnsupportedProjectName(newProjectName, oldProjectState.getProjectName());
                            }
                        } else if (newProjectName.length() == 0) {
                            if(oldProjectState.isActive() && inactive) {
                               //inactive
                                LOG.info("from active to inactive");
                                inactivate(oldProjectState.getProjectName(), snapshot);
                                inactive = false;
                                projectState = ProjectState.createStandbyState();
                            }
                        }
                        success  = true;
                        break;
                    case workerListChanged:
                        if(!projectState.isActive()) {
                            throw new ZKOperationFailedException("Project state is not active.");
                        }
                        if(updateWorkerList) {
                            setWorkerList(agent.fetchWorkerList(projectState.getCurrentWorkerRoot()), snapshot);
                            //updateWorkerList = false;
                            LOG.info("workerList:" + workerList);
                        }
                        if(updateTaskList) {
                            setTaskList(agent.fetchTaskList(projectState.getCurrentTaskRoot()), snapshot);
                            //updateTaskList = false;
                        }
                        if(!workerList.contains(myId)){
                        	LOG.info("current workerlist do not contain myId");
                        	forceExit();
                        }
                        if(recalculate) {
                            recalculateMyTasks();
                            taskToBeOwned = new HashSet<>();
                            taskOwned = new HashSet<>();
                            taskReleased = new HashSet<>();
                            taskToBeOwned.addAll(myTaskList);
                            //recalculate = false;
                        }
                        reassignTasks(taskToBeReleased,taskReleased,taskToBeOwned, taskOwned);
                        success = true;
                        break;
                    case taskListChanged:
                        if(!projectState.isActive()) {
                            throw new ZKOperationFailedException("Project state is not active.");
                        }
                        if(updateWorkerList) {
                            setWorkerList(agent.fetchWorkerList(projectState.getCurrentWorkerRoot()), snapshot);
                            //updateWorkerList = false;
                            LOG.info("workerList:" + workerList);
                        }
                        if(updateTaskList) {
                            setTaskList(agent.fetchTaskList(projectState.getCurrentTaskRoot()), snapshot);
                            //updateTaskList = false;
                        }
                        if(!workerList.contains(myId)){
                        	LOG.info("current workerlist do not contain myId");
                        	forceExit();
                        }
                        if(recalculate) {
                            recalculateMyTasks();
                            taskToBeOwned = new HashSet<>();
                            taskOwned = new HashSet<>();
                            taskReleased = new HashSet<>();
                            taskToBeOwned.addAll(myTaskList);
                            //recalculate = false;
                        }
                        reassignTasks(taskToBeReleased,taskReleased, taskToBeOwned, taskOwned);
                        success = true;
                        break;
                }
            } catch(WrongStateException ex) {
                ex.printStackTrace();   
                --retry;
                LOG.warn("Wrong state! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                Thread.sleep(retryInterval);
            }catch (KeeperException.ConnectionLossException ex){
                ex.printStackTrace();
                --retry;
                LOG.warn("Connection loss! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                Thread.sleep(retryInterval);
            }catch (KeeperException ex){
                ex.printStackTrace();
                agent.reset();
                --retry;
                LOG.warn("Keeper Exception! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                Thread.sleep(retryInterval);
            }catch (ZKOperationFailedException ex) {
                ex.printStackTrace();
                --retry;
                LOG.warn("ZKOperationFailed Exception! Will retry in " + retryInterval + " ms. " + retry + " times left.");
                Thread.sleep(retryInterval);
            } catch (OperationOutOfDateException ex){
                ex.printStackTrace();
                LOG.warn("OperationOutOfDate! " + event.toString() + " will abort!");
                return;
            }catch (InterruptedException ex){
                ex.printStackTrace();
                forceExit();
            } catch (ExecutorException ex) {
                ex.printStackTrace();
                forceExit();
            } finally {
            	taskToBeReleased.addAll(taskOwned);
                if (retry <= 0) {
                    forceExit();
                }
            }
        }
        LOG.info("=============== Handle Event end ===============");
        
    }

    private void recalculateMyTasks() {
        synchronized (rebalanceLock){
            LOG.info("recalculateMyTasks");
            myTaskList = rebalanceTool.calculateMyTask(myId, taskList, workerList);
            LOG.info("calculated tasks: " + myTaskList.toString());
        }
    }

    public void forceExit() {
        LOG.error("Fatal error! Will exit to release resources.");
        try {
            executorFactory.stopCurrentExecutor();
            agent.close();
        } catch (ExecutorException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.exit(-1);
        }
    }

    synchronized void setTaskList(List<String> newCurrentTaskList, ClientDataVersion snapshot){
        this.taskList = newCurrentTaskList;
        currentState.updateTaskListVersion();
        snapshot.updateTaskListVersion();
    }
    synchronized void setWorkerList(List<String> newCurrentWorkerList, ClientDataVersion snapshot){
        this.workerList = newCurrentWorkerList;
        currentState.updateWorkerListVersion();
        snapshot.updateWorkerListVersion();
    }
    List<String> getTaskList(){
        return taskList;
    }
    List<String> getWorkerList() {
        return workerList;
    }
    public static void showUsage() {
        System.out.println("Usage: -n worker_name");
    }

    public static void main(String args[]) {
        try {
            String workerName = null;
            String host = "localhost:2181";
            int timeout = 10000;
            if (args.length == 2) {
                if ("-n".equals(args[0])) {
                    workerName = args[1];
                } else {
                    showUsage();
                }
            } else if (args.length == 4) {
                if ("-n".equals(args[0]) && "-t".equals(args[2])) {
                    workerName = args[1];
                    timeout = Integer.valueOf(args[3]);
                } else if ("-n".equals(args[0]) && "-h".equals(args[2])) {
                    workerName = args[1];
                    host = args[3];
                } else {
                    showUsage();
                    System.exit(1);
                }
            } else if (args.length == 6) {
                if ("-n".equals(args[0]) && "-t".equals(args[2]) && "-h".equals(args[4])) {
                    workerName = args[1];
                    timeout = Integer.valueOf(args[3]);
                    host = args[5];
                } else {
                    showUsage();
                    System.exit(1);
                }

            } else {
                showUsage();
                System.exit(1);
            }
            ExecutorFactory factory = new ExecutorFactory();
//                factory.loadExecutorsFromFile("executors.xml");
            LibraClient watcher = new LibraClient(workerName, host, timeout, new ExecutorFactory());
            watcher.start();
            System.out.println("INFO: Worker " + workerName + " at " + host + " is Running... ");
            Thread.sleep(6000000);
            System.out.println("INFO: Worker " + workerName + "exit.");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }
}
