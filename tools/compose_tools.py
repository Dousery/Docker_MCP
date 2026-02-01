import json
import os
import subprocess
from typing import List, Dict, Any, Optional


COMPOSE_FILES = (
    "docker-compose.yml",
    "docker-compose.yaml",
    "compose.yml",
    "compose.yaml",
)


class ComposeTools:
    """
    Helper class for Docker Compose operations.
    """

    def __init__(self):
        """Initialize Compose CLI (docker compose or docker-compose). Handles detection."""
        self._compose_cmd = self._detect_compose_cmd()

    def _has_compose_file(self, directory: str) -> bool:
        """Return True if directory contains a known compose file."""
        for name in COMPOSE_FILES:
            if os.path.isfile(os.path.join(directory, name)):
                return True
        return False

    def _find_project_dir(self, project_dir: Optional[str] = None) -> str:
        """
        Resolve project directory: use given path, or auto-detect from workspace/cwd.
        When None: checks MCP_PROJECT_DIR / WORKSPACE_ROOT, then searches upward from cwd
        for a directory containing docker-compose.yml (or compose.yml / .yaml).
        """
        if project_dir is not None and project_dir:
            path = os.path.abspath(project_dir)
            if not os.path.isdir(path):
                raise ValueError(f"Project directory not found: {project_dir}")
            if not self._has_compose_file(path):
                raise ValueError(
                    f"No compose file (docker-compose.yml, compose.yml, etc.) in: {project_dir}"
                )
            return path
        # Auto-detect: env (MCP workspace) then walk up from cwd
        for env_key in ("MCP_PROJECT_DIR", "WORKSPACE_ROOT", "CURSOR_WORKSPACE_ROOT"):
            path = os.environ.get(env_key)
            if path and os.path.isdir(path) and self._has_compose_file(path):
                return os.path.abspath(path)
        cwd = os.getcwd()
        path = cwd
        while True:
            if self._has_compose_file(path):
                return os.path.abspath(path)
            parent = os.path.dirname(path)
            if parent == path:
                raise ValueError(
                    "No compose project found. Set project_dir or run from a directory "
                    "containing docker-compose.yml / compose.yml (or set MCP_PROJECT_DIR)."
                )
            path = parent

    def _detect_compose_cmd(self) -> List[str]:
        """Return ['docker', 'compose'] or ['docker-compose'] depending on what's available."""
        try:
            r = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0:
                return ["docker", "compose"]
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        try:
            r = subprocess.run(
                ["docker-compose", "version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if r.returncode == 0:
                return ["docker-compose"]
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        raise RuntimeError(
            "Docker Compose not found. Install 'docker compose' (v2) or 'docker-compose' (v1)."
        )

    def _run_compose(
        self,
        project_dir: str,
        args: List[str],
        capture_output: bool = True,
        timeout: Optional[int] = 120,
    ) -> subprocess.CompletedProcess:
        """Run docker compose in project_dir with given args."""
        if not os.path.isdir(project_dir):
            raise ValueError(f"Project directory not found: {project_dir}")
        cmd = self._compose_cmd + args
        try:
            return subprocess.run(
                cmd,
                cwd=project_dir,
                capture_output=capture_output,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"Compose command timed out: {e}")
        except FileNotFoundError:
            raise RuntimeError("Docker or Compose executable not found.")

    def compose_up(
        self,
        project_dir: Optional[str] = None,
        services: Optional[List[str]] = None,
        build: bool = False,
        detach: bool = True,
    ) -> Dict[str, Any]:
        """
        Start Compose services.

        Args:
            project_dir: Path to directory with docker-compose.yml (default: auto-detect from cwd/MCP)
            services: Optional list of service names to start (default: all)
            build: Build images before starting
            detach: Run in background (default: True)
        """
        project_dir = self._find_project_dir(project_dir)
        args = ["up", "-d"] if detach else ["up"]
        if build:
            args.append("--build")
        if services:
            args.extend(services)
        r = self._run_compose(project_dir, args, capture_output=False)
        if r.returncode != 0:
            raise RuntimeError(
                f"compose up failed (exit {r.returncode}). "
                "Check docker-compose.yml and container logs."
            )
        return {
            "status": "started",
            "project_dir": os.path.abspath(project_dir),
            "services": services or "all",
            "message": "Services started successfully.",
        }

    def compose_down(
        self,
        project_dir: Optional[str] = None,
        volumes: bool = False,
        remove_orphans: bool = False,
    ) -> Dict[str, Any]:
        """
        Stop and remove Compose containers, networks, and optionally volumes.

        Args:
            project_dir: Path to directory with docker-compose.yml (default: auto-detect from cwd/MCP)
            volumes: Remove named volumes declared in the compose file
            remove_orphans: Remove containers for services not defined in the compose file
        """
        project_dir = self._find_project_dir(project_dir)
        args = ["down"]
        if volumes:
            args.append("--volumes")
        if remove_orphans:
            args.append("--remove-orphans")
        r = self._run_compose(project_dir, args)
        if r.returncode != 0:
            raise RuntimeError(f"compose down failed (exit {r.returncode}): {r.stderr or r.stdout}")
        return {
            "status": "stopped",
            "project_dir": os.path.abspath(project_dir),
            "volumes_removed": volumes,
            "message": "Services stopped and removed successfully.",
        }

    def compose_ps(
        self,
        project_dir: Optional[str] = None,
        all_containers: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        List Compose services and their containers.

        Args:
            project_dir: Path to directory with docker-compose.yml (default: auto-detect from cwd/MCP)
            all_containers: Include stopped containers
        """
        project_dir = self._find_project_dir(project_dir)
        args = ["ps", "--format", "json"]
        if all_containers:
            args.append("-a")
        r = self._run_compose(project_dir, args)
        if r.returncode != 0:
            raise RuntimeError(f"compose ps failed (exit {r.returncode}): {r.stderr or r.stdout}")
        # docker compose ps --format json may output one JSON object per line
        result = []
        for line in (r.stdout or "").strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                result.append({
                    "id": obj.get("ID", ""),
                    "name": obj.get("Name", ""),
                    "service": obj.get("Service", ""),
                    "status": obj.get("State", ""),
                    "ports": obj.get("Publishers") or [],
                })
            except json.JSONDecodeError:
                # Fallback: return raw line if not JSON (older compose)
                result.append({"raw": line})
        # If no JSON lines, try parsing table output from older docker-compose
        if not result and (r.stdout or "").strip():
            for line in (r.stdout or "").strip().splitlines():
                result.append({"raw": line})
        return result

    def compose_logs(
        self,
        project_dir: Optional[str] = None,
        services: Optional[List[str]] = None,
        tail: int = 100,
        follow: bool = False,
    ) -> Dict[str, Any]:
        """
        Get Compose service logs.

        Args:
            project_dir: Path to directory with docker-compose.yml (default: auto-detect from cwd/MCP)
            services: Optional list of service names (default: all)
            tail: Number of lines to show from the end of each log (default: 100)
            follow: Stream logs (blocks until interrupted). When True, returns current tail only.
        """
        project_dir = self._find_project_dir(project_dir)
        args = ["logs", "--tail", str(tail)]
        if follow:
            args.append("--follow")
        if services:
            args.extend(services)
        r = self._run_compose(
            project_dir,
            args,
            capture_output=not follow,
            timeout=None if follow else 60,
        )
        if r.returncode != 0 and not follow:
            raise RuntimeError(f"compose logs failed (exit {r.returncode}): {r.stderr or r.stdout}")
        return {
            "project_dir": os.path.abspath(project_dir),
            "services": services or "all",
            "tail": tail,
            "follow": follow,
            "output": (r.stdout or "") if not follow else "(streaming)",
        }

    def compose_scale(
        self,
        service: str,
        count: int,
        project_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Scale a Compose service to a given number of replicas.

        Args:
            service: Service name to scale
            count: Number of replicas (must be >= 0)
            project_dir: Path to directory with docker-compose.yml (default: auto-detect from cwd/MCP)
        """
        project_dir = self._find_project_dir(project_dir)
        if count < 0:
            raise ValueError("Scale count must be >= 0")
        # docker compose up -d --scale service=N
        args = ["up", "-d", "--scale", f"{service}={count}"]
        r = self._run_compose(project_dir, args)
        if r.returncode != 0:
            raise RuntimeError(
                f"compose scale failed (exit {r.returncode}): {r.stderr or r.stdout}. "
                f"Ensure service '{service}' exists and supports scaling."
            )
        return {
            "status": "scaled",
            "project_dir": os.path.abspath(project_dir),
            "service": service,
            "replicas": count,
            "message": f"Service '{service}' scaled to {count} replica(s).",
        }

    def close(self):
        """Close Docker client connection (no-op; Compose uses CLI)."""
        pass
