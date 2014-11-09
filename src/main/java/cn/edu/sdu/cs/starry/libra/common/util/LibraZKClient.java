package cn.edu.sdu.cs.starry.libra.common.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A ZK client for libra
 * @author : xccui
 * Date: 13-10-10
 * Time: 9:27
 */
public class LibraZKClient {
    private static final Logger LOG = LoggerFactory.getLogger(LibraZKClient.class);

    private ZooKeeper zooKeeper;
    private Watcher watcher;
    private String hosts;
    private int sessionTimeout;

    public LibraZKClient(String hosts, int sessionTimeout, Watcher watcher) throws IOException {
        zooKeeper = new ZooKeeper(hosts, sessionTimeout, watcher);
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
        this.watcher = watcher;
    }
    
    public boolean checkNodeExist(String path) throws KeeperException, InterruptedException{
    	return null != zooKeeper.exists(path,watcher);
    }

    public boolean checkAndCreateNode(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
        path = path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        LOG.info("checkAndCreateNode - path:" + path);
        Stat stat = zooKeeper.exists(path, watcher);
        if (null == stat) {
            int lastSlash = path.lastIndexOf('/');
            boolean validSubPath = path.substring(0, lastSlash).length() > 1;
            try {
                if (validSubPath) {
                    checkAndCreateNode(path.substring(0, lastSlash), null, CreateMode.PERSISTENT);
                }
            } catch (KeeperException ex) {
                ex.printStackTrace();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            } finally {
                if (validSubPath) {
                    checkAndCreateNode(path.substring(0, lastSlash), null, CreateMode.PERSISTENT);
                }
            }
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            return false;
        }
        return true;
    }

    public boolean checkAndDeleteNode(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (null == stat) {
            return false;
        }
        zooKeeper.delete(path, stat.getVersion());
        return true;
    }

    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        path = path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return Collections.unmodifiableList(zooKeeper.getChildren(path, watcher));
    }

    public String getDataString(String path, Stat stat) throws KeeperException, InterruptedException {
        return new String(zooKeeper.getData(path, watcher, stat)).trim();
    }

    public String checkAndGetDataString(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return new String(zooKeeper.getData(path, false, stat)).trim();
    }

    public boolean compareAndUpdateData(String path, String comparedStr, byte[] data, boolean watch) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if(null == stat) {
            return false;
        }
        String value = new String(zooKeeper.getData(path, watch, stat)).trim();
        if (value.equals(comparedStr)) {
            zooKeeper.setData(path, data, stat.getVersion());
            return true;
        }
        return false;
    }
    
    public boolean setDataString(String path, String data) throws KeeperException, InterruptedException{
    	Stat stat = zooKeeper.exists(path, watcher);
    	if(null == stat)
    		return false;
    	else{
    		zooKeeper.setData(path, data.getBytes(), stat.getVersion());
    		return true;
    	}   		  	
    }
    
    public long getNodeCreateTime(String path) throws KeeperException, InterruptedException{
    	Stat stat = zooKeeper.exists(path,watcher);
    	if(null != stat){
    	  return stat.getCtime();
    	}
    	return -1l;
    }
    
    public synchronized void close() throws InterruptedException {
        if(null != zooKeeper) {
            zooKeeper.close();
        }
    }
    public synchronized void reconnect() {
        if(null != zooKeeper) {
            try {
                zooKeeper.close();
                zooKeeper = new ZooKeeper(hosts, sessionTimeout, watcher);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
