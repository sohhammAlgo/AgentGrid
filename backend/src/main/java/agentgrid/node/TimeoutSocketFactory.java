package agentgrid.node;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

/**
 * RMI client socket factory with an explicit connect timeout.
 *
 * On Windows, connecting to a localhost port with no listener does not fail at once:
 * the TCP stack retries the SYN and reports "connection refused" only after about 2 s.
 * Heartbeats, election messages and ring forwarding to a killed node would all stall
 * that long. The election service is exported with this factory, and peers look up
 * registries through it, so every election-related connect gives up after
 * CONNECT_TIMEOUT_MS.
 */
public final class TimeoutSocketFactory implements RMIClientSocketFactory, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int CONNECT_TIMEOUT_MS = 300;

    public static final TimeoutSocketFactory INSTANCE = new TimeoutSocketFactory();

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            return socket;
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == TimeoutSocketFactory.class;
    }

    @Override
    public int hashCode() {
        return TimeoutSocketFactory.class.hashCode();
    }
}
