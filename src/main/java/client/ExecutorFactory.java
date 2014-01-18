package client;

import common.exception.ExecutorException;
import common.exception.OperationOutOfDateException;
import common.exception.WrongStateException;
import test.AnotherTestExecutor;
import test.TestExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-14
 * Time: 上午9:43
 * To change this template use File | Settings | File Templates.
 */
public class ExecutorFactory {
    private IExecutor currentExecutor;
    private Map<String, IExecutor> executorMap;
    private state currentState;
    private long taskVersion;

    private enum state {UNLOADED, LOADED, STARTED}


    public ExecutorFactory() {
        executorMap = new HashMap<String, IExecutor>();
        currentState = state.UNLOADED;
        taskVersion = 0;
        executorMap.put("project1", new TestExecutor());
        executorMap.put("project2", new AnotherTestExecutor());
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
        taskVersion++;
        currentState = state.STARTED;
    }

    public synchronized void stopCurrentExecutor() throws WrongStateException {
        if (currentState == state.UNLOADED) {
            throw new WrongStateException("Expected state:" + state.STARTED + ", but got:" + currentState);
        }
        currentExecutor.stop();
        taskVersion++;
        currentState = state.UNLOADED;
    }

    public synchronized List<String> getCurrentTaskList() {
        if (null == currentExecutor || null == currentExecutor.getMyTaskList()) {
            return new ArrayList<String>(0);
        } else {
            return currentExecutor.getMyTaskList();
        }
    }

    public synchronized long getCurrentTaskVersion() {
        return taskVersion;
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
