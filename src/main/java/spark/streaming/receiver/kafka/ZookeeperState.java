package spark.streaming.receiver.kafka;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperState {
	
	public static final Logger log = LoggerFactory.getLogger(ZookeeperState.class);
	
	transient CuratorFramework curator;

	private CuratorFramework newCurator(String quorumList) throws Exception {		
		return CuratorFrameworkFactory.newClient(quorumList, 120000, 120000,
				new RetryNTimes(5, 1000));
	}

	public CuratorFramework getCurator() {
		assert curator != null;
		return curator;
	}

	
	public ZookeeperState(String quorumList) {

		try {
			curator = CuratorFrameworkFactory.newClient(quorumList, 120000,
					120000, new RetryNTimes(5, 1000));
			log.info("Starting curator service");
			curator.start();
		} catch (Exception e) {
			log.error("Curator service not started");
			throw new RuntimeException(e);
		}
	}

	public void writeJSON(String path, Map<Object, Object> data) {
		log.info("Writing " + path + " the data " + data.toString());
		writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
	}

	public void writeBytes(String path, byte[] bytes) {
		try {
			if (curator.checkExists().forPath(path) == null) {
				curator.create().creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT).forPath(path, bytes);
			} else {
				curator.setData().forPath(path, bytes);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void writeTransactionalJSON(String path, Map<Object, Object> data)
	{
		byte[] bytes = JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8"));
		
		try {			
			if (curator.checkExists().forPath(path) == null) {
				curator.create().creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT).forPath(path, bytes);
			} else {
				curator.inTransaction().setData().forPath(path, bytes).and().commit();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}

	public Map<Object, Object> readJSON(String path) {
		try {
			byte[] b = readBytes(path);
			if (b == null) {
				return null;
			}
			return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] readBytes(String path) {
		try {
			if (curator.checkExists().forPath(path) != null) {
				return curator.getData().forPath(path);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		curator.close();
		curator = null;
	}
}
