import docker
from typing import List, Dict, Any, Optional


class ImageTools:
    """
    Helper class for Docker image operations.
    """
    
    def __init__(self):
        """Initialize Docker client. Handles connection errors."""
        try:
            self.client = docker.from_env()
            self.client.ping()
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Docker connection failed: {str(e)}")
    
    def get_image_list(self, all_images: bool = False) -> List[Dict[str, Any]]:
        """
        Get list of all images as JSON-serializable dictionaries.
        
        Args:
            all_images: If True, includes intermediate images (default: False)
        
        Returns:
            List of image dictionaries with id, tags, size, created date and virtual size
        """
        try:
            images = self.client.images.list(all=all_images)
            return [
                {
                    "id": image.id,
                    "tags": image.tags if image.tags else ["<none>"],
                    "size": image.attrs.get("Size", 0),
                    "created": image.attrs.get("Created", ""),
                    "virtual_size": image.attrs.get("VirtualSize", 0),
                }
                for image in images
            ]
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to list images: {str(e)}")
    
    def _find_image(self, image_identifier: str):
        """
        Find image by ID or name/tag (supports partial name matching).
        """
        try:
            return self.client.images.get(image_identifier)
        except docker.errors.NotFound:
            # If not found, try partial name matching
            all_images = self.client.images.list(all=True)
            matching = []
            
            for img in all_images:
                if image_identifier.lower() in img.id.lower():
                    matching.append(img)
                    continue
                
                for tag in (img.tags or []):
                    if image_identifier.lower() in tag.lower():
                        matching.append(img)
                        break
            
            if len(matching) == 1:
                return matching[0]
            elif len(matching) > 1:
                tags_list = []
                for img in matching:
                    tags = img.tags if img.tags else ["<none>"]
                    tags_list.extend(tags)
                unique_tags = list(set(tags_list))[:5]  # Limit to 5 for readability
                raise ValueError(
                    f"Multiple images match '{image_identifier}': {', '.join(unique_tags)}. "
                    f"Please be more specific (use name:tag format)."
                )
            else:
                # Suggest similar names
                all_tags = []
                for img in all_images:
                    if img.tags:
                        all_tags.extend(img.tags)
                
                similar = [
                    tag for tag in all_tags
                    if image_identifier.lower() in tag.lower()
                ]
                suggestion = f" Did you mean: {', '.join(similar[:3])}?" if similar else ""
                raise ValueError(f"Image '{image_identifier}' not found.{suggestion}")
    
    def get_image_info(self, image_identifier: str) -> Dict[str, Any]:
        """
        Get detailed information about an image.
        
        Args:
            image_identifier: Image ID, name:tag, or partial name
                             (e.g., "nginx", "nginx:latest", "ubuntu:20.04", or image ID)
        
        Returns:
            Dictionary with image details including tags, size, architecture, etc.
        """
        try:
            image = self._find_image(image_identifier)
            attrs = image.attrs
            
            return {
                "id": image.id,
                "tags": image.tags if image.tags else ["<none>"],
                "size": attrs.get("Size", 0),
                "virtual_size": attrs.get("VirtualSize", 0),
                "created": attrs.get("Created", ""),
                "architecture": attrs.get("Architecture", "unknown"),
                "os": attrs.get("Os", "unknown"),
                "author": attrs.get("Author", ""),
                "config": {
                    "env": attrs.get("Config", {}).get("Env", []),
                    "cmd": attrs.get("Config", {}).get("Cmd", []),
                    "exposed_ports": attrs.get("Config", {}).get("ExposedPorts", {}),
                },
            }
        except ValueError:
            raise  # Re-raise ValueError as-is (already has good error message)
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get image info: {str(e)}")
    
    def pull_image(self, image_name: str, tag: str = "latest") -> Dict[str, Any]:
        """
        Pull an image from Docker registry (default: Docker Hub).
        
        Args:
            image_name: Image name (e.g., "nginx", "ubuntu", "python")
            tag: Image tag (default: "latest")
        
        Returns:
            Dictionary with pull result information
        """
        try:
            full_name = f"{image_name}:{tag}" if tag else image_name
            image = self.client.images.pull(image_name, tag=tag)
            
            return {
                "id": image.id,
                "tags": image.tags if image.tags else ["<none>"],
                "status": "pulled",
                "message": f"Successfully pulled {full_name}",
            }
        except docker.errors.NotFound:
            raise ValueError(f"Image '{image_name}:{tag}' not found in registry")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to pull image: {str(e)}")
    
    def remove_image(self, image_identifier: str, force: bool = False) -> Dict[str, Any]:
        """
        Remove an image from local Docker host.
        
        Args:
            image_identifier: Image ID, name:tag, or partial name
            force: Force removal even if image is in use (default: False)
        
        Returns:
            Dictionary with removal result
        """
        try:
            image = self._find_image(image_identifier)
            image_tags = image.tags if image.tags else ["<none>"]
            
            self.client.images.remove(image.id, force=force)
            
            return {
                "id": image.id,
                "tags": image_tags,
                "status": "removed",
                "message": f"Successfully removed image: {', '.join(image_tags)}",
            }
        except docker.errors.NotFound:
            raise ValueError(f"Image '{image_identifier}' not found")
        except docker.errors.ImageNotFound:
            raise ValueError(f"Image '{image_identifier}' not found")
        except docker.errors.APIError as e:
            if "image is being used" in str(e).lower():
                raise RuntimeError(
                    f"Image '{image_identifier}' is in use. Use force=True to remove it."
                )
            raise RuntimeError(f"Failed to remove image: {str(e)}")
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to remove image: {str(e)}")
    
    def search_images(self, term: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Search for images on Docker Hub.
        
        Args:
            term: Search term (e.g., "nginx", "python", "postgres")
            limit: Maximum number of results to return (default: 25, max: 100)
        
        Returns:
            List of search results with name, description, star count, etc.
        """
        try:
            limit = min(limit, 100)  # Docker Hub max is 100
            results = self.client.images.search(term, limit=limit)
            
            return [
                {
                    "name": result.get("name", ""),
                    "description": result.get("description", ""),
                    "star_count": result.get("star_count", 0),
                    "is_official": result.get("is_official", False),
                    "is_automated": result.get("is_automated", False),
                }
                for result in results
            ]
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to search images: {str(e)}")
    
    def get_image_history(self, image_identifier: str) -> List[Dict[str, Any]]:
        """
        Get the history of an image (layers and commands).
        
        Args:
            image_identifier: Image ID, name:tag, or partial name
        
        Returns:
            List of history entries with created date, size, and command
        """
        try:
            image = self._find_image(image_identifier)
            history = image.history()
            
            return [
                {
                    "id": entry.get("Id", ""),
                    "created": entry.get("Created", 0),
                    "created_by": entry.get("CreatedBy", ""),
                    "size": entry.get("Size", 0),
                    "comment": entry.get("Comment", ""),
                }
                for entry in history
            ]
        except ValueError:
            raise  # Re-raise ValueError as-is
        except docker.errors.DockerException as e:
            raise RuntimeError(f"Failed to get image history: {str(e)}")
    
    def close(self):
        """Close Docker client connection."""
        if hasattr(self, "client"):
            self.client.close()
