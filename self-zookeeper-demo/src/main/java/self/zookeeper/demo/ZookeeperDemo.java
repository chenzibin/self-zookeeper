package self.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZookeeperDemo
 *
 * @author chenzb
 * @date 2020/5/8
 */
public class ZookeeperDemo {

    private ZooKeeper zooKeeper;
    private Stat stat = new Stat();

    public ZooKeeper connect() throws IOException, InterruptedException {
        String connectString = "47.98.193.129:2181";
        int sessionTimeOut = 10000;
        MyWatcher watcher = new MyWatcher();
        zooKeeper = new ZooKeeper(connectString, sessionTimeOut, watcher);
        watcher.syncConnected();
        return zooKeeper;
    }

    public String createNode() throws KeeperException, InterruptedException {
        return zooKeeper.create("/zk-demo", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public String createAsyncNode() throws KeeperException, InterruptedException {
        MyStringCallback callback = new MyStringCallback();
        zooKeeper.create("/zk-demo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, callback, "i am context");
        return callback.getResult();
    }

    public List<String> getChildren() throws KeeperException, InterruptedException {
        List<String> nodes = zooKeeper.getChildren("/", true);
        nodes.forEach(System.out::println);
        return nodes;
    }

    public List<String> getAsyncChildren() throws KeeperException, InterruptedException {
        MyChildrenCallback callback = new MyChildrenCallback();
        zooKeeper.getChildren("/", true, callback, null);
        List<String> nodes = callback.getResult();
        nodes.forEach(System.out::println);
        return nodes;
    }

    public byte[] getData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/zk-demo", true, stat);
        System.out.println(new String(data));
        System.out.println(stat);
        return data;
    }

    private void setData() throws KeeperException, InterruptedException {
        zooKeeper.setData("/zk-demo", "134".getBytes(), -1);
    }

    class MyWatcher implements Watcher {
        private CountDownLatch connectedSemaphore = new CountDownLatch(1);

        void syncConnected() throws InterruptedException {
            connectedSemaphore.await();
        }

        @Override
        public void process(WatchedEvent event) {
            System.out.println(event);
            switch (event.getState()) {
                case Disconnected:
                    break;
                case SyncConnected:
                    connectedSemaphore.countDown();
                    switch (event.getType()) {
                        case NodeChildrenChanged:
                            try {
                                getChildren();
                            } catch (KeeperException | InterruptedException e) {
                                e.printStackTrace();
                            }
                            break;
                        case NodeDataChanged:
                            try {
                                getData();
                            } catch (KeeperException | InterruptedException e) {
                                e.printStackTrace();
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                case AuthFailed:
                    break;
                case ConnectedReadOnly:
                    break;
                case SaslAuthenticated:
                    break;
                case Expired:
                    break;
                case Closed:
                    break;
                default:
                    break;
            }
        }
    }

    class MyStringCallback implements AsyncCallback.StringCallback {
        private String result;

        private CountDownLatch createdSemaphore = new CountDownLatch(1);

        void syncCreated() throws InterruptedException {
            createdSemaphore.await();
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            this.result = String.format("create path result: %s, %s, %s, real path name: %s", rc, path, ctx, name);
            createdSemaphore.countDown();
        }

        public String getResult() throws InterruptedException {
            syncCreated();
            return result;
        }
    }

    class MyChildrenCallback implements AsyncCallback.Children2Callback {
        private List<String> result;

        private CountDownLatch childrenSyncSemaphore = new CountDownLatch(1);

        void syncChildren() throws InterruptedException {
            childrenSyncSemaphore.await();
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            this.result = children;
            String output = String.format("create path result: %s, %s, %s, stat: %s", rc, path, ctx, stat);
            System.out.println(output);
            childrenSyncSemaphore.countDown();
        }

        public List<String> getResult() throws InterruptedException {
            syncChildren();
            return result;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperDemo zookeeperDemo = new ZookeeperDemo();
        zookeeperDemo.connect();
        zookeeperDemo.createNode();
        zookeeperDemo.getData();
        zookeeperDemo.setData();
        zookeeperDemo.getAsyncChildren();
        zookeeperDemo.createAsyncNode();
    }
}
