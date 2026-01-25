import docker
from typing import List, Dict, Any, Optional


class NetworkTools:
    """
    Helper class for Docker network operations.
    """
    
    def __init__(self):
        """Initialize Docker client. Handles connection errors."""
        try:
            self.client = docker.from_env()
            self.client.ping()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker connection failed: {str(e)}")
    
    def get_network_list(self) -> List[Dict[str, Any]]:
        """Get list of all networks as JSON-serializable dictionaries."""
        try:
            networks = self.client.networks.list()
            return [
                {
                    "id": network.id,
                    "name": network.name,
                    "driver": network.attrs.get("Driver", "unknown"),
                    "scope": network.attrs.get("Scope", "unknown"),
                    "internal": network.attrs.get("Internal", False),
                    "containers": list(network.attrs.get("Containers", {}).keys()),
                }
                for network in networks
            ]
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to list networks: {str(e)}")
    
    def _find_network(self, network_identifier: str):
        """
        Find network by ID or name (supports partial name matching).
        """
        try:
            return self.client.networks.get(network_identifier)
        except docker.errors.NotFound:
            all_networks = self.client.networks.list()
            matching = [
                n for n in all_networks
                if network_identifier.lower() in n.name.lower()
                or network_identifier.lower() in n.id.lower()
            ]
            
            if len(matching) == 1:
                return matching[0]
            elif len(matching) > 1:
                names = [n.name for n in matching]
                raise ValueError(
                    f"Multiple networks match '{network_identifier}': {', '.join(names)}. "
                    f"Please be more specific."
                )
            else:
                all_names = [n.name for n in all_networks]
                similar = [name for name in all_names if network_identifier.lower() in name.lower()]
                suggestion = f" Did you mean: {', '.join(similar[:3])}?" if similar else ""
                raise ValueError(f"Network '{network_identifier}' not found.{suggestion}")
    
    def get_network_info(self, network_identifier: str) -> Dict[str, Any]:
        """
        Get detailed information about a network.
        
        Args:
            network_identifier: Network ID, full name, or partial name
        """
        try:
            network = self._find_network(network_identifier)
            attrs = network.attrs
            
            # Get connected containers info
            containers_info = []
            for container_id, container_data in attrs.get("Containers", {}).items():
                containers_info.append({
                    "id": container_id,
                    "name": container_data.get("Name", ""),
                    "ipv4_address": container_data.get("IPv4Address", ""),
                    "ipv6_address": container_data.get("IPv6Address", ""),
                })
            
            return {
                "id": network.id,
                "name": network.name,
                "driver": attrs.get("Driver", "unknown"),
                "scope": attrs.get("Scope", "unknown"),
                "internal": attrs.get("Internal", False),
                "attachable": attrs.get("Attachable", False),
                "created": attrs.get("Created", ""),
                "ipam": attrs.get("IPAM", {}),
                "containers": containers_info,
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get network info: {str(e)}")
    
    def create_network(
        self,
        name: str,
        driver: str = "bridge",
        internal: bool = False,
        attachable: bool = True,
        labels: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new Docker network.
        
        Args:
            name: Network name
            driver: Network driver (bridge, overlay, host, none, macvlan)
            internal: If True, network is isolated from external networks
            attachable: If True, containers can be attached manually
            labels: Optional labels for the network
        """
        try:
            network = self.client.networks.create(
                name=name,
                driver=driver,
                internal=internal,
                attachable=attachable,
                labels=labels or {}
            )
            
            return {
                "id": network.id,
                "name": network.name,
                "driver": driver,
                "status": "created",
                "message": f"Successfully created network '{name}'",
            }
        except docker.errors.APIError as e:
            if "already exists" in str(e).lower():
                raise ValueError(f"Network '{name}' already exists")
            raise RuntimeError(f"Failed to create network: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to create network: {str(e)}")
    
    def remove_network(self, network_identifier: str) -> Dict[str, Any]:
        """
        Remove a Docker network.
        
        Args:
            network_identifier: Network ID, full name, or partial name
        """
        try:
            network = self._find_network(network_identifier)
            network_name = network.name
            network_id = network.id
            
            network.remove()
            
            return {
                "id": network_id,
                "name": network_name,
                "status": "removed",
                "message": f"Successfully removed network '{network_name}'",
            }
        except ValueError:
            raise
        except docker.errors.APIError as e:
            if "has active endpoints" in str(e).lower():
                raise RuntimeError(
                    f"Network '{network_identifier}' has connected containers. "
                    f"Disconnect them first or use force."
                )
            raise RuntimeError(f"Failed to remove network: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to remove network: {str(e)}")
    
    def connect_container(
        self,
        network_identifier: str,
        container_identifier: str,
        ipv4_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Connect a container to a network.
        
        Args:
            network_identifier: Network ID or name
            container_identifier: Container ID or name
            ipv4_address: Optional static IPv4 address
        """
        try:
            network = self._find_network(network_identifier)
            
            # Find container
            try:
                container = self.client.containers.get(container_identifier)
            except docker.errors.NotFound:
                raise ValueError(f"Container '{container_identifier}' not found")
            
            # Connect with optional IP
            if ipv4_address:
                network.connect(container, ipv4_address=ipv4_address)
            else:
                network.connect(container)
            
            return {
                "network": network.name,
                "container": container.name,
                "ipv4_address": ipv4_address or "auto-assigned",
                "status": "connected",
                "message": f"Connected '{container.name}' to network '{network.name}'",
            }
        except ValueError:
            raise
        except docker.errors.APIError as e:
            if "already exists" in str(e).lower():
                raise ValueError(
                    f"Container '{container_identifier}' is already connected to network '{network_identifier}'"
                )
            raise RuntimeError(f"Failed to connect container: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to connect container: {str(e)}")
    
    def disconnect_container(
        self,
        network_identifier: str,
        container_identifier: str,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Disconnect a container from a network.
        
        Args:
            network_identifier: Network ID or name
            container_identifier: Container ID or name
            force: Force disconnect even if container is running
        """
        try:
            network = self._find_network(network_identifier)
            
            # Find container
            try:
                container = self.client.containers.get(container_identifier)
            except docker.errors.NotFound:
                raise ValueError(f"Container '{container_identifier}' not found")
            
            network.disconnect(container, force=force)
            
            return {
                "network": network.name,
                "container": container.name,
                "status": "disconnected",
                "message": f"Disconnected '{container.name}' from network '{network.name}'",
            }
        except ValueError:
            raise
        except docker.errors.APIError as e:
            if "is not connected" in str(e).lower():
                raise ValueError(
                    f"Container '{container_identifier}' is not connected to network '{network_identifier}'"
                )
            raise RuntimeError(f"Failed to disconnect container: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to disconnect container: {str(e)}")
    
    def prune_networks(self) -> Dict[str, Any]:
        """
        Remove all unused networks.
        
        Returns information about removed networks.
        """
        try:
            result = self.client.networks.prune()
            
            return {
                "networks_deleted": result.get("NetworksDeleted", []) or [],
                "status": "pruned",
                "message": f"Removed {len(result.get('NetworksDeleted', []) or [])} unused networks",
            }
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to prune networks: {str(e)}")
    
    def close(self):
        """Close Docker client connection."""
        if hasattr(self, "client"):
            self.client.close()
