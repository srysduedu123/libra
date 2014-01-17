package client;

import common.util.LibraZKPathUtil;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-13
 * Time: 下午12:04
 * To change this template use File | Settings | File Templates.
 */
public class ProjectStat {
    private String myActiveWorkerRoot;
    private String myTaskRoot;
    private String myProjectName;

    private ProjectStat() {
        standby();
    }

    public static ProjectStat createStandbyState() {
        ProjectStat projectStat = new ProjectStat();
        return projectStat;
    }

    private void active(final String projectName) {
        myProjectName = projectName;
        myActiveWorkerRoot = LibraZKPathUtil.genActiveWorkerRootPath(projectName);
        myTaskRoot = LibraZKPathUtil.genTaskRootPath(projectName);
    }

    public String getMyProjectName() {
        return myProjectName;
    }

    public String getMyActiveWorkerRoot() {
        return myActiveWorkerRoot;
    }

    public String getMyTaskRoot() {
        return myTaskRoot;
    }

    public boolean isActive() {
        return null != myProjectName && myProjectName.trim().length() > 0;
    }


    public static ProjectStat createActiveState(final String projectName) {
        ProjectStat projectStat = new ProjectStat();
        projectStat.active(projectName);
        return projectStat;
    }


    private void standby() {
        this.myProjectName = "";
        this.myActiveWorkerRoot = LibraZKPathUtil.UNSET_ACTIVE_WORKER_ROOT;
        this.myTaskRoot = LibraZKPathUtil.UNSET_TASK_ROOT;
    }

    public boolean isNewProjectName(String newProjectName) {
        return !this.myProjectName.equals(newProjectName);
    }
}
