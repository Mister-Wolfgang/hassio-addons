"""Arenti Talk media_player platform."""
from __future__ import annotations
import logging
import asyncio
import httpx

from homeassistant.components.media_player import (
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
    MediaType,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SUPPORT_ARENTI = (
    MediaPlayerEntityFeature.PLAY_MEDIA
    | MediaPlayerEntityFeature.STOP
    | MediaPlayerEntityFeature.VOLUME_SET
    | MediaPlayerEntityFeature.VOLUME_STEP
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    api_url = entry.data["api_url"].rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{api_url}/cameras")
            cameras = r.json()
    except Exception as e:
        _LOGGER.error("Cannot reach Arenti Talk API: %s", e)
        return

    entities = [ArentiMediaPlayer(api_url, name, info) for name, info in cameras.items()]
    async_add_entities(entities, update_before_add=True)


class ArentiMediaPlayer(MediaPlayerEntity):
    _attr_media_content_type = MediaType.MUSIC
    _attr_should_poll = False

    def __init__(self, api_url: str, camera_name: str, info: dict) -> None:
        self._api_url = api_url
        self._camera_name = camera_name
        self._attr_name = f"Arenti {camera_name.replace('_', ' ').title()}"
        self._attr_unique_id = f"arenti_talk_{camera_name}"
        self._attr_state = MediaPlayerState.IDLE
        self._attr_volume_level = 0.5
        self._attr_supported_features = SUPPORT_ARENTI

    async def async_play_media(self, media_type: str, media_id: str, **kwargs) -> None:
        """Play TTS text or audio URL."""
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                if media_type == MediaType.MUSIC and media_id.startswith("http"):
                    # Download and POST as file
                    resp = await client.get(media_id)
                    resp.raise_for_status()
                    files = {"file": ("audio.mp3", resp.content, resp.headers.get("content-type", "audio/mpeg"))}
                    await client.post(f"{self._api_url}/talk/{self._camera_name}", files=files)
                else:
                    # Treat as TTS text
                    await client.post(
                        f"{self._api_url}/tts/{self._camera_name}",
                        json={"text": media_id, "lang": "fr"},
                    )
            self._attr_state = MediaPlayerState.PLAYING
            self.async_write_ha_state()
        except Exception as e:
            _LOGGER.error("[%s] play_media failed: %s", self._camera_name, e)

    async def async_media_stop(self) -> None:
        self._attr_state = MediaPlayerState.IDLE
        self.async_write_ha_state()

    async def async_set_volume_level(self, volume: float) -> None:
        self._attr_volume_level = volume
        self.async_write_ha_state()

    @property
    def state(self) -> MediaPlayerState:
        return self._attr_state
