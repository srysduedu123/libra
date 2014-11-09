package cn.edu.sdu.cs.starry.libra.client;

import cn.edu.sdu.cs.starry.libra.common.exception.ExecutorException;
import cn.edu.sdu.cs.starry.libra.common.exception.OperationOutOfDateException;
import cn.edu.sdu.cs.starry.libra.common.exception.WrongStateException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A factory for all registered executors
 * @author : xccui
 * Date: 13-10-14
 * Time: 9:43
 */
public class ExecutorFactory {
    private IExecutor currentExecutor;
    private Map<String, IExecutor> executorMap;
    private state currentState;
    private long executorVersion;

    private enum state {UNLOADED, LOADED, STARTED};


    public ExecutorFactory() {
        executorMap = new HashMap<String, IExecutor>();
        currentState = state.UNLOADED;
        executorVersion = 0;
    }

    public void loadExecutorsFromFile(String executorConfFile){

    }

    public void addExecutor(String name, IExecutor executor){
       executorMap.put(name, executor);
    }
    public synchronized void loadExecutor(String projectName) throws ExecutorException {
        if (currentState != state.UNLOADED) {
            throw new WrongStateException("Expected state:" + state.UNLOADED + ", but got:" + currentState);
        }
        currentExecutor = executorMap.get(projectName);
        currentExecutor.prepare();
        currentState = state.LOADED;
    }


    public synchronized void pauseCurrentExecutor(String projectName) throws ExecutorException, OperationOutOfDateException {
        checkIsCurrentProject(projectName);
        if (currentState == state.LOADED) {
            return;
        }
        if (currentState != state.STARTED) {
            throw new WrongStateException("Expected state:" + state.STARTED + " or " + state.LOADED + ", but got:" + currentState);
        }
        currentExecutor.pause();
        currentState = state.LOADED;
    }

    public synchronized void startCurrentExecutor(String projectName, List<String> taskList) throws OperationOutOfDateException, WrongStateException {
        checkIsCurrentProject(projectName);
        if (currentState != state.LOADED) {
            throw new WrongStateException("Expected state:" + state.LOADED + ", but got:" + currentState);
        }
        currentExecutor.start(taskList);
        executorVersion++;
        currentState = state.STARTED;
    }

    public synchronized void stopCurrentExecutor() throws WrongStateException {
        currentExecutor.stop();
        executorVersion++;
        currentState = state.UNLOADED;
    }

    public synchronized List<String> getCurrentTaskList() {
        if (null == currentExecutor || null == currentExecutor.getMyTaskList()) {
            return new ArrayList<>(0);
        } else {
            return currentExecutor.getMyTaskList();
        }
    }

    public boolean isProjectSupported(String projectName) {
        return projectName.length() == 0 || executorMap.containsKey(projectName);
    }

    private void checkIsCurrentProject(String projectName) throws OperationOutOfDateException {
        if (!currentExecutor.getProjectName().equals(projectName)) {
            throw new OperationOutOfDateException("Operating projectName:" + projectName + ". Current projectName:" + currentExecutor.getProjectName());
        }
    }
}
