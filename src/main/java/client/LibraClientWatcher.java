package client;

import common.util.LibraZKPathUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author xccui
 * Date: 14-10-20
 * Time: 8:58
 */
public class LibraClientWatcher implements Watcher {
    private static Logger LOG = LoggerFactory.getLogger(LibraClientWatcher.class);
    private LibraClient client;

    public LibraClientWatcher(LibraClient client) {
        this.client = client;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("zkWatcher process:" + watchedEvent.getState() + "\t" + watchedEvent.getPath() + "\t" + watchedEvent.getType());
        if (watchedEvent.getState().equals(Event.KeeperState.Disconnected)) {
            LOG.warn("Zookeeper disconnected! Will try to reconnect!");
            client.restart();
        } else {
            try{
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    if (watchedEvent.getPath().contains(client.projectState.getCurrentWorkerRoot())) {
                        // current worker list changed
                        client.handleEvent(LibraClient.LibraClientEvent.workerListChanged);
                        LOG.info("worker changed");
                    } else if (watchedEvent.getPath().contains(client.projectState.getCurrentTaskRoot())) {
                        // current task list changed
                        client.handleEvent(LibraClient.LibraClientEvent.taskListChanged);
                        LOG.info("task changed");
                    }
                } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged && watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {
                    // my project was changed
                    client.handleEvent(LibraClient.LibraClientEvent.projectChanged);
                } else if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().contains(LibraZKPathUtil.ALL_WORKER_ROOT)) {
                    // this client was removed by server
                    client.forceExit();
                }
            }catch(InterruptedException ex){
                ex.printStackTrace();
                LOG.warn("Process interrupted!");
            }
        }
    }
}