/**
 * 
 */
package net.sf.jabb.dstream.eventhub;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;

/**
 * @author James Hu
 *
 */
public class EventHubQpidConnectionFactory extends ConnectionFactoryImpl {
	protected String toStringContent;

	public EventHubQpidConnectionFactory(String protocol, String host, int port, String username, String password, String clientId,
			String remoteHost, boolean ssl, int maxSessions) {
		this(protocol, host, port, username, password, clientId, remoteHost, ssl, maxSessions, null);
	}

	public EventHubQpidConnectionFactory(String protocol, String host, int port, String username, String password, String clientId,
										 String remoteHost, boolean ssl, int maxSessions, String eventHubName) {
		this(protocol, host, port, username, password, clientId, remoteHost, ssl, maxSessions, eventHubName, null);
	}

	public EventHubQpidConnectionFactory(String protocol, String host, int port, String username, String password, String clientId,
			String remoteHost, boolean ssl, int maxSessions, String eventHubName, Boolean syncPublish) {
		super(protocol, host, port, username, password, clientId, remoteHost, ssl, maxSessions);
		toStringContent = (username == null ? "" : username) + "@" + host + "/" + (eventHubName == null ? "?" : eventHubName)
				+ "<-" + clientId + "(syncPublish=" + syncPublish + ")";
		setSyncPublish(syncPublish);
	}

	@Override
	public String toString(){
		return toStringContent;
	}
}
