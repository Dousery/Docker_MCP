import docker
from typing import List, Dict, Any, Optional


class ContainerTools:
    """
    Helper class for Docker container operations.
    """
    
    def __init__(self):
        """Initialize Docker client. Handles connection errors."""
        try:
            self.client = docker.from_env()
            # Test connection
            self.client.ping()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker connection failed: {str(e)}")
    
    def get_container_list(self) -> List[Dict[str, Any]]:
        """Get list of all containers as JSON-serializable dictionaries."""
        try:
            containers = self.client.containers.list(all=True)
            return [
                {
                    "id": container.id,
                    "name": container.name,
                    "status": container.status,
                    "image": container.image.tags[0] if container.image.tags else "unknown",
                }
                for container in containers
            ]
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to list containers: {str(e)}")
    
    def _find_container(self, container_identifier: str):
        """
        Find container by ID or name (supports partial name matching).
        Docker API accepts both ID and name, but we also support partial name matching
        for better natural language query support.
        """
        try:
            # First try direct lookup (works for both ID and exact name)
            return self.client.containers.get(container_identifier)
        except docker.errors.NotFound:
            # If not found, try partial name matching
            all_containers = self.client.containers.list(all=True)
            matching = [
                c for c in all_containers
                if container_identifier.lower() in c.name.lower()
                or container_identifier.lower() in c.id.lower()
            ]
            
            if len(matching) == 1:
                return matching[0]
            elif len(matching) > 1:
                names = [c.name for c in matching]
                raise ValueError(
                    f"Multiple containers match '{container_identifier}': {', '.join(names)}. "
                    f"Please be more specific."
                )
            else:
                # Suggest similar names
                all_names = [c.name for c in all_containers]
                similar = [name for name in all_names if container_identifier.lower() in name.lower()]
                suggestion = f" Did you mean: {', '.join(similar[:3])}?" if similar else ""
                raise ValueError(f"Container '{container_identifier}' not found.{suggestion}")
    
    def get_container_info(self, container_identifier: str) -> Dict[str, Any]:
        """
        Get detailed information about a container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
                                  (e.g., "nginx", "my-nginx-container", or container ID)
        """
        try:
            container = self._find_container(container_identifier)
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "image": container.image.tags[0] if container.image.tags else "unknown",
                "created": container.attrs.get("Created", ""),
                "ports": container.attrs.get("NetworkSettings", {}).get("Ports", {}),
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get container info: {str(e)}")
    
    def get_container_logs(self, container_identifier: str, tail: int = 100) -> str:
        """
        Get container logs as a string.
        
        Args:
            container_identifier: Container ID, full name, or partial name
            tail: Number of log lines to retrieve (default: 100)
        """
        try:
            container = self._find_container(container_identifier)
            logs = container.logs(tail=tail, timestamps=True).decode("utf-8")
            return logs
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get container logs: {str(e)}")
    
    def get_container_stats(self, container_identifier: str) -> Dict[str, Any]:
        """
        Get container statistics.
        
        Args:
            container_identifier: Container ID, full name, or partial name
        """
        try:
            container = self._find_container(container_identifier)
            stats = container.stats(stream=False)
            # Extract and format key metrics
            return {
                "cpu_percent": self._calculate_cpu_percent(stats),
                "memory_usage": stats.get("memory_stats", {}).get("usage", 0),
                "memory_limit": stats.get("memory_stats", {}).get("limit", 0),
                "network_io": stats.get("networks", {}),
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get container stats: {str(e)}")
    
    def _calculate_cpu_percent(self, stats: Dict[str, Any]) -> float:
        """Calculate CPU usage percentage from stats."""
        try:
            cpu_delta = (
                stats["cpu_stats"]["cpu_usage"]["total_usage"]
                - stats.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
            )
            system_delta = (
                stats["cpu_stats"]["system_cpu_usage"]
                - stats.get("precpu_stats", {}).get("system_cpu_usage", 0)
            )
            if system_delta > 0:
                return (cpu_delta / system_delta) * 100.0
            return 0.0
        except (KeyError, ZeroDivisionError):
            return 0.0
    
    def close(self):
        """Close Docker client connection."""
        if hasattr(self, "client"):
            self.client.close()