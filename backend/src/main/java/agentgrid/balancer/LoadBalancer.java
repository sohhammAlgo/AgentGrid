package agentgrid.balancer;

import agentgrid.rmi.AgentService;

import java.rmi.RemoteException;
import java.util.List;

/**
 * A pluggable routing policy: given the current pool of agent nodes,
 * pick the one that should receive the next subtask.
 *
 * This is deliberately an interface so the Orchestrator (or, here, the
 * demo client standing in for one) can swap policies with a one-line
 * change -- which is what makes the Exp 6 comparison table possible in
 * the first place.
 */
public interface LoadBalancer {

    AgentService select(List<AgentService> nodes) throws RemoteException;

    String getName();
}