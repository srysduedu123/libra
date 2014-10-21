package test;

import client.IExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-14
 * Time: 上午9:50
 * To change this template use File | Settings | File Templates.
 */
public class AnotherTestExecutor implements IExecutor {
    private static Logger LOG = LoggerFactory.getLogger(AnotherTestExecutor.class);
    private List<String> taskList;

    @Override
    public void prepare() {
        LOG.info("prepare");
    }

    @Override
    public void start(List<String> taskList) {
        StringBuilder sb = new StringBuilder();
        for (String task : taskList) {
            sb.append(task + ",");
        }
        this.taskList = taskList;
        LOG.info("sleeping 3 seconds before starting");
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("start:" + sb.toString());
    }

    @Override
    public void pause() {
        LOG.info("sleeping 3 seconds before pausing");
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("pause");
    }

    @Override
    public void stop() {
        LOG.info("sleeping 3 seconds before stopping");
        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("stop");
    }

    @Override
    public String getProjectName() {
        return "project2";
    }

    @Override
    public List<String> getMyTaskList() {
        return taskList;
    }
}
