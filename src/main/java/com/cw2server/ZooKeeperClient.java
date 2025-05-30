package com.cw2server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient {
    private final ZooKeeper zooKeeper;

    public ZooKeeperClient(String zooKeeperUrl, int sessionTimeout, Watcher watcher) throws IOException {
        zooKeeper = new ZooKeeper(zooKeeperUrl, sessionTimeout, watcher);
    }

    public String createNode(String path, boolean shouldWatch, CreateMode mode, byte[] data)
            throws KeeperException, InterruptedException {
        String createdPath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);

        // Set a watch on it for future change
        if (shouldWatch) {
            zooKeeper.exists(createdPath, true);
            zooKeeper.getData(createdPath, true, null);
        }

        return createdPath;
    }

    public boolean CheckExists(String path) throws KeeperException, InterruptedException {
        Stat nodeStat = zooKeeper.exists(path, false);
        return (nodeStat != null);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, -1);
    }

    public List<String> getChildrenNodePaths (String root) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(root, false);
    }

    public void addWatch(String path) throws KeeperException, InterruptedException {
        zooKeeper.exists(path, true);
    }
    public byte[] getData(String path, boolean shouldWatch) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, shouldWatch, null);
    }

    public void write(String path, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data, -1);
    }
    public void forceDelete(String path) throws KeeperException, InterruptedException {
        ZKUtil.deleteRecursive(zooKeeper, path);
    }
}