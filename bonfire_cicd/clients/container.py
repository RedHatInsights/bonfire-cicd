import logging
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import attr
from docker import DockerClient
from docker.errors import BuildError as DockerBuildError
from docker.models.images import Image as DockerImage
from docker.utils import parse_repository_tag
from invoke import run
from invoke.exceptions import UnexpectedExit

log = logging.getLogger(__name__)


@attr.s
class ContainerClient:
    base_url: Optional[str] = attr.ib(default=None)
    client: Union["PodmanClient", DockerClient] = attr.ib()

    @client.default
    def _client_factory(self):
        if self.podman_available():
            return PodmanClient()
        return DockerClient(base_url=self.base_url)

    @classmethod
    def from_env(cls) -> "ContainerClient":
        client = PodmanClient.from_env() if cls.podman_available() else DockerClient.from_env()
        return cls(client=client)

    @staticmethod
    def podman_available() -> bool:
        try:
            return bool(run("which podman"))
        except UnexpectedExit:
            return False

    def login(
        self,
        username: str,
        password: str,
        registry: str,
        **kwargs,
    ) -> Union[Dict[str, Any], str]:
        return self.client.login(username=username, password=password, registry=registry, **kwargs)

    def pull(
        self, repository: str, tag: Optional[str] = None, all_tags: bool = False, **kwargs
    ) -> Union[Union[DockerImage, List[DockerImage]], str]:
        return self.client.images.pull(repository, tag, all_tags, **kwargs)

    def build(
        self, path: str, tag: str, dockerfile: str, **kwargs
    ) -> Union[Tuple[DockerImage, Iterator[bytes]], str]:
        if kwargs.get("cache_from"):
            log.info("Attempting to build image using cache")
            repository, __ = parse_repository_tag(tag)
            self.pull(repository=repository, cache_from=True)
            try:
                return self.client.images.build(
                    path=path,
                    tag=tag,
                    dockerfile=dockerfile,
                    cache_from=[kwargs["cache_from"]],
                )
            except DockerBuildError:
                log.info("Build from cache failed, attempting build without cache")
                pass
        return self.client.images.build(
            path=path,
            tag=tag,
            dockerfile=dockerfile,
        )

    def push(
        self, repository: str, tag: Optional[str] = None, **kwargs
    ) -> Union[Union[str, Iterator[Union[str, Dict[str, Any]]]], str]:
        return self.client.images.push(repository=repository, tag=tag, **kwargs)


class PodmanClient:
    """Podman client using system podman."""

    def __init__(self) -> None:
        self.images = PodmanImages()
        self.containers = PodmanContainers()

    @classmethod
    def from_env(cls) -> "PodmanClient":
        """Just return PodmanClient for consistency with DockerClient."""
        return cls()

    def login(
        self,
        username: str,
        password: str,
        registry: str,
        **kwargs,
    ) -> str:
        """Run podman login."""
        result = run(f"podman login -u={username!r} -p={password!r} {registry}")
        return result.stdout


class PodmanImages:
    @staticmethod
    def pull(repository: str, tag: Optional[str] = None, all_tags: bool = False, **kwargs) -> str:
        """Run podman pull."""
        all_tags_arg = "--all-tags" if all_tags else ""
        tag_arg = f":{tag}" or ""
        result = run(f"podman pull {repository}{tag_arg} {all_tags_arg}")
        return result.stdout

    @staticmethod
    def build(path: str, tag: str, dockerfile: str, **kwargs) -> str:
        """Run podman build."""
        try:
            result = run(f"podman build -f {path}/{dockerfile} -t {tag} {path}")
        except Exception as err:
            raise DockerBuildError(str(err), None)
        return result.stdout

    @staticmethod
    def push(repository: str, tag: Optional[str] = None, **kwargs) -> str:
        """Run podman push."""
        result = run(f"podman push {repository}:{tag or 'latest'}")
        return result.stdout


class PodmanContainers:
    @staticmethod
    def run(
        image,
        command,
        network_mode,
        name,
        entrypoint,
        tty,
    ):
        tty_arg = "-t" if tty else ""
        run(
            f"podman run {tty_arg} --net={network_mode} --name={name}"
            f" --entrypoint={entrypoint} {image} -c {command}"
        )

    @staticmethod
    def cp(name, from_path, to_path):
        run(f"podman cp {name}:{from_path} {to_path}")
