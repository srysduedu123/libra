package cn.edu.sdu.cs.starry.libra.common.util;

/**
 * ZK path utility for libra
 * @author  xccui
 * Date: 13-10-10
 * Time: 10:09
 */
public class LibraZKPathUtil {
    public static final String ROOT = "/starry/Libra";
    public static final String PROJECT_ROOT = ROOT + "/projects";
    public static final String ALL_WORKER_ROOT = ROOT + "/workers/all";
    public static final String CONFIG_ROOT = ROOT + "/config";
    public static final String UNSET_TASK_ROOT = ROOT + "/unsetTask";
    public static final String UNSET_ACTIVE_WORKER_ROOT = ROOT + "/unsetActiveWorker";

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
