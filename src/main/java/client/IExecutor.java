package client;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-9
 * Time: 下午5:04
 * To change this template use File | Settings | File Templates.
 */
public interface IExecutor {
    public void prepare();

    public void start(List<String> taskList);

    public void pause();

    public void stop();

    public String getProjectName();

    public List<String> getMyTaskList();
}
