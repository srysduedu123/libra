package cn.edu.sdu.cs.starry.libra.client;

import java.util.List;

/**
 * Interface for project executor
 * @author  xccui
 * Date: 13-10-9
 * Time: 17:04
 */
public interface IExecutor {
    public void prepare();

    public void start(List<String> taskList);

    public void pause();

    public void stop();

    public String getProjectName();

    public List<String> getMyTaskList();
}
