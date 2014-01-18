package common.entity;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-12
 * Time: 下午9:28
 * To change this template use File | Settings | File Templates.
 */
public class TaskAndWorker implements Comparable<TaskAndWorker> {
    public String task;
    public String worker;

    public TaskAndWorker(String task, String worker) {
        this.task = task;
        this.worker = worker;
    }
    
	@Override
    public int compareTo(TaskAndWorker o) {
        return task.compareTo(o.task);
    }
}
