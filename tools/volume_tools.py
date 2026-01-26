import docker
from typing import List, Dict, Any, Optional


class VolumeTools:
    """
    Helper class for Docker volume operations.
    """
    
    def __init__(self):
        """Initialize Docker client. Handles connection errors."""
        try:
            self.client = docker.from_env()
            self.client.ping()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker connection failed: {str(e)}")
    
    def get_volume_list(self) -> List[Dict[str, Any]]:
        """Get list of all volumes as JSON-serializable dictionaries."""
        try:
            volumes = self.client.volumes.list()
            return [
                {
                    "name": volume.name,
                    "driver": volume.attrs.get("Driver", "unknown"),
                    "mountpoint": volume.attrs.get("Mountpoint", ""),
                    "scope": volume.attrs.get("Scope", "local"),
                    "created": volume.attrs.get("CreatedAt", ""),
                    "labels": volume.attrs.get("Labels", {}),
                }
                for volume in volumes
            ]
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to list volumes: {str(e)}")
    
    def _find_volume(self, volume_identifier: str):
        """
        Find volume by name (supports partial name matching).
        """
        try:
            return self.client.volumes.get(volume_identifier)
        except docker.errors.NotFound:
            all_volumes = self.client.volumes.list()
            matching = [
                v for v in all_volumes
                if volume_identifier.lower() in v.name.lower()
            ]
            
            if len(matching) == 1:
                return matching[0]
            elif len(matching) > 1:
                names = [v.name for v in matching]
                raise ValueError(
                    f"Multiple volumes match '{volume_identifier}': {', '.join(names)}. "
                    f"Please be more specific."
                )
            else:
                all_names = [v.name for v in all_volumes]
                similar = [name for name in all_names if volume_identifier.lower() in name.lower()]
                suggestion = f" Did you mean: {', '.join(similar[:3])}?" if similar else ""
                raise ValueError(f"Volume '{volume_identifier}' not found.{suggestion}")
    
    def get_volume_info(self, volume_identifier: str) -> Dict[str, Any]:
        """
        Get detailed information about a volume.
        
        Args:
            volume_identifier: Volume name or partial name
        """
        try:
            volume = self._find_volume(volume_identifier)
            attrs = volume.attrs
            
            return {
                "name": volume.name,
                "driver": attrs.get("Driver", "unknown"),
                "mountpoint": attrs.get("Mountpoint", ""),
                "scope": attrs.get("Scope", "local"),
                "created": attrs.get("CreatedAt", ""),
                "labels": attrs.get("Labels", {}),
                "options": attrs.get("Options", {}),
                "status": attrs.get("Status", {}),
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get volume info: {str(e)}")
    
    def create_volume(
        self,
        name: str,
        driver: str = "local",
        driver_opts: Optional[Dict[str, str]] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new Docker volume.
        
        Args:
            name: Volume name
            driver: Volume driver (default: local)
            driver_opts: Optional driver-specific options
            labels: Optional labels for the volume
        """
        try:
            volume = self.client.volumes.create(
                name=name,
                driver=driver,
                driver_opts=driver_opts or {},
                labels=labels or {}
            )
            
            return {
                "name": volume.name,
                "driver": driver,
                "mountpoint": volume.attrs.get("Mountpoint", ""),
                "status": "created",
                "message": f"Successfully created volume '{name}'",
            }
        except docker.errors.APIError as e:
            if "already exists" in str(e).lower():
                raise ValueError(f"Volume '{name}' already exists")
            raise RuntimeError(f"Failed to create volume: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to create volume: {str(e)}")
    
    def remove_volume(self, volume_identifier: str, force: bool = False) -> Dict[str, Any]:
        """
        Remove a Docker volume.
        
        Args:
            volume_identifier: Volume name or partial name
            force: Force removal even if volume is in use
        """
        try:
            volume = self._find_volume(volume_identifier)
            volume_name = volume.name
            
            volume.remove(force=force)
            
            return {
                "name": volume_name,
                "status": "removed",
                "message": f"Successfully removed volume '{volume_name}'",
            }
        except ValueError:
            raise
        except docker.errors.APIError as e:
            if "volume is in use" in str(e).lower():
                raise RuntimeError(
                    f"Volume '{volume_identifier}' is in use. Use force=True to remove it."
                )
            raise RuntimeError(f"Failed to remove volume: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to remove volume: {str(e)}")
    
    def prune_volumes(self) -> Dict[str, Any]:
        """
        Remove all unused volumes.
        
        Returns information about removed volumes and space reclaimed.
        """
        try:
            result = self.client.volumes.prune()
            
            volumes_deleted = result.get("VolumesDeleted", []) or []
            space_reclaimed = result.get("SpaceReclaimed", 0)
            
            return {
                "volumes_deleted": volumes_deleted,
                "space_reclaimed_bytes": space_reclaimed,
                "space_reclaimed_mb": round(space_reclaimed / (1024 * 1024), 2),
                "status": "pruned",
                "message": f"Removed {len(volumes_deleted)} unused volumes, reclaimed {round(space_reclaimed / (1024 * 1024), 2)} MB",
            }
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to prune volumes: {str(e)}")
    
    def get_volumes_by_container(self, container_identifier: str) -> List[Dict[str, Any]]:
        """
        Get volumes attached to a specific container.
        
        Args:
            container_identifier: Container ID, full name, or partial name
        """
        try:
            container = self._find_container(container_identifier)
            mounts = container.attrs.get("Mounts", [])
            
            return [
                {
                    "name": mount.get("Name", ""),
                    "type": mount.get("Type", ""),
                    "source": mount.get("Source", ""),
                    "destination": mount.get("Destination", ""),
                    "mode": mount.get("Mode", ""),
                    "read_write": mount.get("RW", True),
                }
                for mount in mounts
            ]
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get container volumes: {str(e)}")
    
    def _find_container(self, container_identifier: str):
        """
        Find container by ID or name (supports partial name matching).
        """
        try:
            return self.client.containers.get(container_identifier)
        except docker.errors.NotFound:
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
                raise ValueError(f"Container '{container_identifier}' not found.")
    
    def get_volume_usage(self, volume_identifier: str) -> Dict[str, Any]:
        """
        Get which containers are using a specific volume.
        
        Args:
            volume_identifier: Volume name or partial name
        """
        try:
            volume = self._find_volume(volume_identifier)
            volume_name = volume.name
            
            # Find all containers using this volume
            containers_using = []
            all_containers = self.client.containers.list(all=True)
            
            for container in all_containers:
                mounts = container.attrs.get("Mounts", [])
                for mount in mounts:
                    if mount.get("Name") == volume_name or mount.get("Source") == volume.attrs.get("Mountpoint"):
                        containers_using.append({
                            "id": container.id,
                            "name": container.name,
                            "status": container.status,
                            "mount_destination": mount.get("Destination", ""),
                            "read_write": mount.get("RW", True),
                        })
                        break
            
            return {
                "volume_name": volume_name,
                "mountpoint": volume.attrs.get("Mountpoint", ""),
                "containers_count": len(containers_using),
                "containers": containers_using,
                "in_use": len(containers_using) > 0,
            }
        except ValueError:
            raise
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get volume usage: {str(e)}")
    
    def backup_volume(
        self,
        volume_identifier: str,
        backup_path: str,
        container_image: str = "alpine"
    ) -> Dict[str, Any]:
        """
        Backup a volume to a tar file using a temporary container.
        
        Args:
            volume_identifier: Volume name or partial name
            backup_path: Host path for backup file (e.g., "/backups/my-volume.tar")
            container_image: Image to use for backup (default: alpine)
        """
        try:
            volume = self._find_volume(volume_identifier)
            volume_name = volume.name
            
            # Run a temporary container to create the backup
            container = self.client.containers.run(
                image=container_image,
                command=f"tar cvf /backup/backup.tar -C /volume .",
                volumes={
                    volume_name: {"bind": "/volume", "mode": "ro"},
                    backup_path.rsplit("/", 1)[0] or ".": {"bind": "/backup", "mode": "rw"}
                },
                remove=True,
                detach=False
            )
            
            return {
                "volume_name": volume_name,
                "backup_path": backup_path,
                "status": "backed_up",
                "message": f"Successfully backed up volume '{volume_name}' to '{backup_path}'",
            }
        except ValueError:
            raise
        except docker.errors.ImageNotFound:
            raise ValueError(f"Image '{container_image}' not found. Try pulling it first.")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to backup volume: {str(e)}")
    
    def close(self):
        """Close Docker client connection."""
        if hasattr(self, "client"):
            self.client.close()
