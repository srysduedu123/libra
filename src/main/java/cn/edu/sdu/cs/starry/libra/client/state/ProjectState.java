package cn.edu.sdu.cs.starry.libra.client.state;

import cn.edu.sdu.cs.starry.libra.common.util.LibraZKPathUtil;

/**
 * Current project state for worker
 * @author xccui
 * Date: 13-10-13
 * Time: 12:04
 */
public class ProjectState {
    private String currentWorkerRoot;
    private String currentTaskRoot;
    private String projectName;

    private ProjectState() {
        standby();
    }

    public static ProjectState createStandbyState() {
        ProjectState projectState = new ProjectState();
        return projectState;
    }

    public static ProjectState createActiveState(final String projectName) {
        ProjectState projectState = new ProjectState();
        projectState.active(projectName);
        return projectState;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getCurrentWorkerRoot() {
        return currentWorkerRoot;
    }

    public String getCurrentTaskRoot() {
        return currentTaskRoot;
    }

    public boolean isActive() {
        return null != projectName && projectName.trim().length() > 0;
    }

    public boolean isNewProjectName(String newProjectName) {
        return !this.projectName.equals(newProjectName);
    }

    private void standby() {
        this.projectName = "";
        this.currentWorkerRoot = LibraZKPathUtil.UNSET_ACTIVE_WORKER_ROOT;
        this.currentTaskRoot = LibraZKPathUtil.UNSET_TASK_ROOT;
    }

    private void active(final String projectName) {
        this.projectName = projectName;
        currentWorkerRoot = LibraZKPathUtil.genActiveWorkerRootPath(projectName);
        currentTaskRoot = LibraZKPathUtil.genTaskRootPath(projectName);
    }

}
