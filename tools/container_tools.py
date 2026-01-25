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
    
    def run_container(
        self,
        image: str,
        name: Optional[str] = None,
        command: Optional[str] = None,
        ports: Optional[Dict[str, int]] = None,
        volumes: Optional[Dict[str, str]] = None,
        environment: Optional[Dict[str, str]] = None,
        network: Optional[str] = None,
        detach: bool = True,
        remove: bool = False,
        restart_policy: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create and run a new container.
        
        Args:
            image: Image name (e.g., "nginx", "nginx:latest", "python:3.9")
            name: Optional container name
            command: Optional command to run
            ports: Port mapping {container_port: host_port} (e.g., {"80/tcp": 8080})
            volumes: Volume mapping {volume_name: container_path} (e.g., {"my-vol": "/data"})
            environment: Environment variables {key: value}
            network: Network to connect to
            detach: Run in background (default: True)
            remove: Auto-remove when stopped (default: False)
            restart_policy: Restart policy (no, always, on-failure, unless-stopped)
        """
        try:
            # Prepare port bindings
            port_bindings = None
            if ports:
                port_bindings = {k: v for k, v in ports.items()}
            
            # Prepare volume bindings
            volume_bindings = None
            if volumes:
                volume_bindings = {k: {"bind": v, "mode": "rw"} for k, v in volumes.items()}
            
            # Prepare restart policy
            restart_policy_dict = None
            if restart_policy:
                restart_policy_dict = {"Name": restart_policy}
            
            container = self.client.containers.run(
                image=image,
                name=name,
                command=command,
                ports=port_bindings,
                volumes=volume_bindings,
                environment=environment,
                network=network,
                detach=detach,
                remove=remove,
                restart_policy=restart_policy_dict
            )
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "image": image,
                "message": f"Successfully started container '{container.name}'",
            }
        except docker.errors.ImageNotFound:
            raise ValueError(f"Image '{image}' not found. Try pulling it first.")
        except docker.errors.APIError as e:
            if "name is already in use" in str(e).lower():
                raise ValueError(f"Container name '{name}' is already in use")
            raise RuntimeError(f"Failed to run container: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to run container: {str(e)}")
    
    def start_container(self, container_identifier: str) -> Dict[str, Any]:
        """
        Start a stopped container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
        """
        try:
            container = self._find_container(container_identifier)
            
            if container.status == "running":
                return {
                    "id": container.id,
                    "name": container.name,
                    "status": "running",
                    "message": f"Container '{container.name}' is already running",
                }
            
            container.start()
            container.reload()
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "message": f"Successfully started container '{container.name}'",
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to start container: {str(e)}")
    
    def stop_container(self, container_identifier: str, timeout: int = 10) -> Dict[str, Any]:
        """
        Stop a running container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
            timeout: Seconds to wait before killing (default: 10)
        """
        try:
            container = self._find_container(container_identifier)
            
            if container.status != "running":
                return {
                    "id": container.id,
                    "name": container.name,
                    "status": container.status,
                    "message": f"Container '{container.name}' is not running",
                }
            
            container.stop(timeout=timeout)
            container.reload()
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "message": f"Successfully stopped container '{container.name}'",
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to stop container: {str(e)}")
    
    def restart_container(self, container_identifier: str, timeout: int = 10) -> Dict[str, Any]:
        """
        Restart a container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
            timeout: Seconds to wait before killing (default: 10)
        """
        try:
            container = self._find_container(container_identifier)
            container.restart(timeout=timeout)
            container.reload()
            
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "message": f"Successfully restarted container '{container.name}'",
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to restart container: {str(e)}")
    
    def remove_container(
        self,
        container_identifier: str,
        force: bool = False,
        remove_volumes: bool = False
    ) -> Dict[str, Any]:
        """
        Remove a container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
            force: Force remove even if running (default: False)
            remove_volumes: Remove associated volumes (default: False)
        """
        try:
            container = self._find_container(container_identifier)
            container_name = container.name
            container_id = container.id
            
            container.remove(force=force, v=remove_volumes)
            
            return {
                "id": container_id,
                "name": container_name,
                "status": "removed",
                "message": f"Successfully removed container '{container_name}'",
            }
        except ValueError:
            raise
        except docker.errors.APIError as e:
            if "is running" in str(e).lower():
                raise RuntimeError(
                    f"Container '{container_identifier}' is running. Use force=True to remove it."
                )
            raise RuntimeError(f"Failed to remove container: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to remove container: {str(e)}")
    
    def exec_in_container(
        self,
        container_identifier: str,
        command: str,
        workdir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a command inside a running container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
            command: Command to execute (e.g., "ls -la", "cat /etc/hosts")
            workdir: Working directory inside container
        """
        try:
            container = self._find_container(container_identifier)
            
            if container.status != "running":
                raise RuntimeError(
                    f"Container '{container_identifier}' is not running. Start it first."
                )
            
            exit_code, output = container.exec_run(
                cmd=command,
                workdir=workdir
            )
            
            return {
                "container": container.name,
                "command": command,
                "exit_code": exit_code,
                "output": output.decode("utf-8") if output else "",
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to execute command: {str(e)}")
    
    def prune_containers(self) -> Dict[str, Any]:
        """
        Remove all stopped containers.
        
        Returns information about removed containers and space reclaimed.
        """
        try:
            result = self.client.containers.prune()
            
            containers_deleted = result.get("ContainersDeleted", []) or []
            space_reclaimed = result.get("SpaceReclaimed", 0)
            
            return {
                "containers_deleted": containers_deleted,
                "space_reclaimed_bytes": space_reclaimed,
                "space_reclaimed_mb": round(space_reclaimed / (1024 * 1024), 2),
                "status": "pruned",
                "message": f"Removed {len(containers_deleted)} stopped containers",
            }
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to prune containers: {str(e)}")
    
    def close(self):
        """Close Docker client connection."""
        if hasattr(self, "client"):
            self.client.close()