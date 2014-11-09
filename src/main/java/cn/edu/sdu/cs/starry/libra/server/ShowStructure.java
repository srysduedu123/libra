package cn.edu.sdu.cs.starry.libra.server;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import cn.edu.sdu.cs.starry.libra.common.util.LibraZKClient;
import cn.edu.sdu.cs.starry.libra.common.util.LibraZKPathUtil;

/**
 * 
 * @author :SDU.llzhang
 * @version 1.0.0 
 * Date: 14-1-17 Time: 3:00PM
 */
public class ShowStructure implements Watcher {

	private String hosts;
	private int sessionTimeout;
	private LibraZKClient zkClient;

	private String root = LibraZKPathUtil.ROOT;
	private List<String> allProjectsList;
	private List<String> allWorkersList;
	private List<String> myTasksList;
	private List<String> myWorkersList;

	public ShowStructure(String hosts, int sessionTimeout)
			throws KeeperException, InterruptedException {
		this.hosts = hosts;
		this.sessionTimeout = sessionTimeout;
		try {
			start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void start() throws IOException, KeeperException,
			InterruptedException {
		zkClient = new LibraZKClient(hosts, sessionTimeout, this);
	}

	private void readStructure(String path) throws KeeperException,
			InterruptedException, IOException {
		if (path.endsWith("projects")) {
			allProjectsList = zkClient.getChildren(path);
		} else if (path.endsWith("all")) {
			allWorkersList = zkClient.getChildren(path);
		}
		List<String> pathList = zkClient.getChildren(path);
		if (!pathList.isEmpty()) {
			for (String s : pathList) {
				readStructure(path + "/" + s);
			}
		}
	}

	private void workerAndTaskPair(String path) throws KeeperException,
			InterruptedException, IOException {
		List<String> pathList = zkClient.getChildren(path);
		if (!pathList.isEmpty()) {
			for (String s : pathList) {
				if (s.endsWith("tasks")) {
					myTasksList = zkClient.getChildren(path + "/" + s);
				} else if (s.endsWith("workers")) {
					myWorkersList = zkClient.getChildren(path + "/" + s);
				}
			}
		}

	}

	public List<String> getAllProjects() throws KeeperException,
			InterruptedException, IOException {
		readStructure(root);
		return allProjectsList;
	}

	public List<String> getAllWorkers() throws KeeperException,
			InterruptedException, IOException {
		readStructure(root);
		return allWorkersList;
	}

	public List<String> getProjectTasks(String projectName)
			throws KeeperException, InterruptedException, IOException {
		workerAndTaskPair(LibraZKPathUtil.PROJECT_ROOT + "/" + projectName);
		return myTasksList;
	}

	public List<String> getWorkerTasks(String workerName)
			throws KeeperException, InterruptedException, IOException {
        List<String> onWorkerTasksList = new LinkedList<>();
		String projectName = zkClient.checkAndGetDataString(LibraZKPathUtil
				.genMyAllWorkerPath(workerName));
		workerAndTaskPair(LibraZKPathUtil.PROJECT_ROOT + "/" + projectName);
		for (String task : myTasksList) {
			String workerId = zkClient.checkAndGetDataString(LibraZKPathUtil
					.genSingleTaskPath(task, projectName));
			if (workerId == workerName) {
				onWorkerTasksList.add(task);
			}
		}
		return onWorkerTasksList;
	}

	public List<String> getProjectWorkers(String projectName)
			throws KeeperException, InterruptedException, IOException {
		workerAndTaskPair(LibraZKPathUtil.PROJECT_ROOT + "/" + projectName);
		return myWorkersList;
	}

	public boolean addProject(String projectName) throws KeeperException,
			InterruptedException {
		return zkClient.checkAndCreateNode(LibraZKPathUtil.PROJECT_ROOT + "/"
				+ projectName, projectName.getBytes(), CreateMode.PERSISTENT);
	}

	public boolean deleteProject(String projectName) throws KeeperException,
			InterruptedException {
		return zkClient.checkAndDeleteNode(LibraZKPathUtil.PROJECT_ROOT + "/"
				+ projectName);
	}

	public boolean addTask(String projectName, String taskName)
			throws KeeperException, InterruptedException {
		return zkClient.checkAndCreateNode(LibraZKPathUtil
				.genTaskRootPath(projectName)
				+ "/" + taskName, new byte[0], CreateMode.PERSISTENT);
	}

	public boolean deleteTask(String projectName, String taskName)
			throws KeeperException, InterruptedException {
		return zkClient.checkAndDeleteNode(LibraZKPathUtil
				.genTaskRootPath(projectName)
				+ "/" + taskName);
	}

	public boolean assignWorker(String workerId, String projectName)
			throws KeeperException, InterruptedException, IOException {
		return zkClient.setDataString(LibraZKPathUtil
				.genMyAllWorkerPath(workerId), projectName);
	}

	public boolean evacuateWorker(String workerId, String projectName)
			throws KeeperException, InterruptedException {
		return zkClient.setDataString(LibraZKPathUtil
				.genMyAllWorkerPath(workerId), "");
	}

	@Override
	public void process(WatchedEvent event) {

	}

	public static void main(String[] args) throws IOException, KeeperException,
			InterruptedException {
		ShowStructure server = new ShowStructure("121.250.213.114:2181", 10000);

		server.addProject("project1");
		server.addTask("project1", "task1");
		server.addTask("project1", "task1");
		server.assignWorker("worker1", "project1");

		server.addProject("project2");
		server.addTask("project2", "task2");
		server.assignWorker("worker2", "project2");
		for(String task:server.getWorkerTasks("worker1")){
			System.out.println(task+",");
		}

		// server.evacuateWorker("worker1", "project1");
		// server.evacuateWorker("worker2", "project2");
	}
}
