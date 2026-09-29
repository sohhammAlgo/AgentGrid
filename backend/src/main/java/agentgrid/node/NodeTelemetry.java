package agentgrid.node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Remote interface through which the control plane pulls a node's recorded events.
 * Bound as "telemetry" in each node's registry.
 */
public interface NodeTelemetry extends Remote {

    /**
     * Returns the buffered events with seq greater than sinceSeq, in seq order.
     * A cursor ahead of this node's latest seq belongs to an earlier incarnation of the
     * node, so the whole buffer is returned instead.
     */
    List<NodeEvent> getEvents(long sinceSeq) throws RemoteException;
}
