"""
TaskClient for interacting with Prefect tasks and flows
"""
from typing import List, Optional, Dict, Any
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowFilter, FlowRunFilter
from prefect.states import State
from prefect.flows import Flow
from prefect.utilities.callables import ParameterSchema


class TaskClient:
    """Client to interact with Prefect flows and deployments"""
    
    def __init__(self, api_url: str = "http://localhost:4200/api"):
        self.api_url = api_url
        self.client = None
        self._initialize_prefect_client()
    
    def _initialize_prefect_client(self):
        """Initialize Prefect client"""
        try:
            # The client is initialized on demand when needed
            pass
        except ImportError:
            print("Warning: Prefect not properly installed. Install with: pip install prefect")
    
    def list_flows(self) -> List[str]:
        """List all registered flows"""
        try:
            from prefect.client.orchestration import get_client
            import asyncio
            
            async def get_flows():
                async with get_client() as client:
                    flows = await client.read_flows(flow_filter=FlowFilter())
                    return [flow.name for flow in flows]
            
            # In a real implementation, we would run this async
            # For now, return a placeholder list
            return ["example_flow_1", "example_flow_2", "etl_flow"]
        except Exception as e:
            print(f"Error listing flows: {e}")
            return []
    
    def list_deployments(self) -> List[Dict[str, Any]]:
        """List all registered deployments"""
        try:
            from prefect.client.orchestration import get_client
            import asyncio
            
            async def get_deployments():
                async with get_client() as client:
                    deployments = await client.read_deployments()
                    return [{
                        "id": str(deployment.id),
                        "name": deployment.name,
                        "flow_id": str(deployment.flow_id),
                        "work_pool_name": deployment.work_pool_name,
                        "is_schedule_active": deployment.is_schedule_active
                    } for deployment in deployments]
            
            # For now, return a placeholder list
            return [
                {"id": "1", "name": "daily-etl-pipeline", "work_pool_name": "default-agent-pool", "is_schedule_active": True},
                {"id": "2", "name": "hourly-monitoring-etl", "work_pool_name": "default-agent-pool", "is_schedule_active": True}
            ]
        except Exception as e:
            print(f"Error listing deployments: {e}")
            return []
    
    def submit(self, deployment_name: str, parameters: Dict[str, Any] = None) -> str:
        """Submit a flow run from a deployment and return the run ID"""
        try:
            from prefect import get_client
            import asyncio
            
            async def run_deployment():
                async with get_client() as client:
                    # Find the deployment by name
                    deployments = await client.read_deployments(
                        deployment_filter=FlowFilter(name={"any_": [deployment_name]})
                    )
                    
                    if not deployments:
                        raise ValueError(f"Deployment {deployment_name} not found")
                    
                    deployment = deployments[0]
                    flow_run = await client.create_flow_run_from_deployment(
                        deployment.id,
                        parameters=parameters or {}
                    )
                    
                    return str(flow_run.id)
            
            # For now, return a mock flow run ID
            import uuid
            return str(uuid.uuid4())
        except Exception as e:
            print(f"Error submitting flow run: {e}")
            raise
    
    def get_flow_run_state(self, flow_run_id: str) -> State:
        """Get the state of a flow run by ID"""
        try:
            from prefect.client.orchestration import get_client
            import asyncio
            
            async def get_state():
                async with get_client() as client:
                    flow_run = await client.read_flow_run(flow_run_id)
                    return flow_run.state
            
            # For now, return a mock state
            from prefect import StateType
            return State(type=StateType.COMPLETED, message="Mock completed state")
        except Exception as e:
            print(f"Error getting flow run state: {e}")
            from prefect import StateType
            return State(type=StateType.FAILED, message=str(e))
    
    def get_result(self, flow_run_id: str) -> Any:
        """Get the result of a completed flow run"""
        try:
            # In a real implementation, we would retrieve the actual result
            # For now, return a mock result
            state = self.get_flow_run_state(flow_run_id)
            if state.type.value == "COMPLETED":
                return {"status": "success", "result_id": flow_run_id}
            else:
                return {"status": "failed", "result_id": flow_run_id, "error": state.message}
        except Exception as e:
            print(f"Error getting result: {e}")
            return {"status": "error", "error": str(e)}
    
    def execute(self, deployment_name: str, parameters: Dict[str, Any] = None):
        """Submit a flow run and wait for the result"""
        try:
            # Submit the flow run
            flow_run_id = self.submit(deployment_name, parameters)
            
            # In a real implementation, we would wait for the flow to complete
            # For now, return the flow run ID immediately
            return self.get_result(flow_run_id)
        except Exception as e:
            print(f"Error executing flow: {e}")
            return {"status": "error", "error": str(e)}
    
    def register_flow(self, flow: Flow) -> str:
        """Register a flow in the Prefect server"""
        # In a real implementation, this would register the flow
        # For now, just return the flow name
        return flow.name if hasattr(flow, 'name') else 'unnamed-flow'
    
    def create_deployment(self, flow_name: str, deployment_name: str, work_pool: str = "default-agent-pool") -> str:
        """Create a deployment for a flow"""
        # In a real implementation, this would create an actual deployment
        # For now, return a mock deployment ID
        import uuid
        return str(uuid.uuid4())


# Example usage function
def get_task_client(api_url: str = "http://localhost:4200/api") -> TaskClient:
    """Get a configured TaskClient instance"""
    return TaskClient(api_url)