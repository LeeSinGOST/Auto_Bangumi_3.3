"""番剧文件处理完成后，HTTP POST 回调外部云端上传脚本。

外部脚本（Docker 部署）在收到请求后负责将已重命名的番剧文件上传到云端
（如阿里云盘、OneDrive、Google Drive 等）。

设计原则：
- 外部脚本调用必须是「尽力而为」：失败仅记日志，**绝不阻塞**
  重命名主流程、通知发送或后续种子处理。
- 单次 HTTP 调用的超时、重试完全在本模块内控制，不依赖调用方保证。
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from module.conf import settings
from module.models.bangumi import Notification
from module.network.request_url import RequestURL, get_shared_client

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RenamedFile:
    """单个已重命名媒体/字幕文件的描述。"""

    path: str
    type: str = "media"  # "media" | "subtitle"
    size: int | None = None


@dataclass(slots=True)
class CloudUploadPayload:
    """发送给外部云端脚本的结构化 payload。

    字段尽量保持自描述，便于外部脚本不依赖 AB 源码也能正确处理。
    """

    event: str = "bangumi.renamed"
    timestamp: float = field(default_factory=time.time)
    official_title: str = ""
    season: int = 0
    episode: int | float = 0
    episode_type: str = "episode"  # "episode" | "movie" | "special"
    poster_url: str | None = None
    torrent_hash: str | None = None
    torrent_name: str | None = None
    save_path: str | None = None
    download_root: str = ""
    renamed_files: list[RenamedFile] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["renamed_files"] = [asdict(f) for f in self.renamed_files]
        return data

    @classmethod
    def from_notification(
        cls,
        notification: Notification,
        *,
        download_root: str = "",
        torrent_hash: str | None = None,
        torrent_name: str | None = None,
        save_path: str | None = None,
        renamed_files: list[RenamedFile] | None = None,
        episode_type: str = "episode",
    ) -> "CloudUploadPayload":
        return cls(
            official_title=notification.official_title,
            season=notification.season,
            episode=notification.episode,
            poster_url=notification.poster_path or None,
            download_root=download_root,
            torrent_hash=torrent_hash,
            torrent_name=torrent_name,
            save_path=save_path,
            renamed_files=renamed_files or [],
            episode_type=episode_type,
        )


class CloudUploader:
    """封装对外部云端脚本的一次或多次 HTTP POST 调用。"""

    def __init__(self) -> None:
        self._cfg = settings.cloud_upload

    @property
    def enabled(self) -> bool:
        return self._cfg.enable and bool(self._cfg.webhook_url)

    async def upload_one(
        self, payload: CloudUploadPayload, *, retries: int = 2
    ) -> bool:
        """发送单条云端上传回调。

        Args:
            payload: 结构化的重命名完成事件。
            retries: 首次失败后的重试次数（0 = 不重试）。

        Returns:
            任一次请求返回 2xx 时为 True，否则为 False。
        """
        if not self.enabled:
            return False
        url = self._cfg.webhook_url
        timeout = self._cfg.timeout
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": RequestURL.DEFAULT_UA,
        }
        if self._cfg.auth_token:
            headers["Authorization"] = f"Bearer {self._cfg.auth_token}"

        body: dict[str, Any] = payload.to_dict()
        if not self._cfg.include_file_paths:
            body.pop("renamed_files", None)
            body.pop("save_path", None)

        client = await get_shared_client()
        for attempt in range(retries + 1):
            try:
                resp = await client.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=timeout,
                )
            except Exception as e:  # network errors, timeouts, ...
                logger.warning(
                    "Cloud upload request failed (attempt %d/%d) %s: %s",
                    attempt + 1,
                    retries + 1,
                    payload.official_title or "<no title>",
                    e,
                )
                if attempt < retries:
                    import asyncio

                    await asyncio.sleep(min(2.0 * attempt + 1.0, 5.0))
                continue
            if 200 <= resp.status_code < 300:
                logger.info(
                    "Cloud upload webhook sent: %s S%sE%s -> HTTP %s",
                    payload.official_title or "<no title>",
                    payload.season,
                    payload.episode,
                    resp.status_code,
                )
                return True
            logger.warning(
                "Cloud upload webhook rejected (HTTP %s): %s. Body preview: %s",
                resp.status_code,
                payload.official_title or "<no title>",
                resp.text[:200],
            )
            if attempt < retries:
                import asyncio

                await asyncio.sleep(min(2.0 * attempt + 1.0, 5.0))
        logger.error(
            "Cloud upload webhook permanently failed for %s S%sE%s",
            payload.official_title or "<no title>",
            payload.season,
            payload.episode,
        )
        return False

    async def upload_many(
        self, payloads: list[CloudUploadPayload]
    ) -> int:
        """并发发送多条云端回调，返回成功数量。

        与通知管理器的广播策略相同：单条失败绝不阻塞其他条目。
        """
        if not self.enabled or not payloads:
            return 0

        import asyncio

        results = await asyncio.gather(
            *[self.upload_one(p) for p in payloads],
            return_exceptions=True,
        )
        ok = 0
        for r in results:
            if r is True:
                ok += 1
            elif isinstance(r, Exception):
                logger.warning("Cloud upload task raised: %r", r)
        return ok
