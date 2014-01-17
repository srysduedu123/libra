package common.util;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-10
 * Time: 下午10:09
 * To change this template use File | Settings | File Templates.
 */
public class LibraZKPathUtil {
    public static final String ROOT = "/starryLibra";
    public static final String PROJECT_ROOT = "/starryLibra/projects";
    public static final String ALL_WORKER_ROOT = ROOT + "/workers/all";
    public static final String CONFIG_ROOT = ROOT + "/config";
    public static final String UNSET_TASK_ROOT = ROOT + "/unsetTask";
    public static final String UNSET_ACTIVE_WORKER_ROOT = ROOT + "/unsetActiveWorker";
    public static final String REBALANCE_MODE_PATH = CONFIG_ROOT + "/rebalanceMode";

    public static String genTaskRootPath(String projectName) {
        return PROJECT_ROOT + "/" + projectName + "/tasks";
    }

    public static String genActiveWorkerRootPath(String projectName) {
        return PROJECT_ROOT + "/" + projectName + "/workers";
    }

    public static String genMyAllWorkerPath(String myId) {
        return ALL_WORKER_ROOT + "/" + myId;
    }

    public static String genMyActiveWorkerPath(String myId, String projectName) {
        return genActiveWorkerRootPath(projectName) + "/" + myId;
    }

    public static String genSingleTaskPath(String taskName, String projectName) {
        return genTaskRootPath(projectName) + "/" + taskName;
    }

}
