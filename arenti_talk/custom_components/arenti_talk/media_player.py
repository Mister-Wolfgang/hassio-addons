"""Arenti Talk media_player platform."""
from __future__ import annotations
import logging

from homeassistant.components.media_player import (
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
    MediaType,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SUPPORT_ARENTI = (
    MediaPlayerEntityFeature.PLAY_MEDIA
    | MediaPlayerEntityFeature.STOP
    | MediaPlayerEntityFeature.VOLUME_SET
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    api_url = entry.data["api_url"].rstrip("/")
    session = async_get_clientsession(hass)
    try:
        async with session.get(f"{api_url}/cameras") as r:
            cameras = await r.json()
    except Exception as e:
        _LOGGER.error("Cannot reach Arenti Talk API at %s: %s", api_url, e)
        return

    entities = [ArentiMediaPlayer(hass, api_url, name) for name in cameras]
    async_add_entities(entities, update_before_add=False)
    _LOGGER.info("Arenti Talk: added %d media_player entities", len(entities))


class ArentiMediaPlayer(MediaPlayerEntity):
    _attr_media_content_type = MediaType.MUSIC
    _attr_should_poll = False

    def __init__(self, hass: HomeAssistant, api_url: str, camera_name: str) -> None:
        self.hass = hass
        self._api_url = api_url
        self._camera_name = camera_name
        self._attr_name = f"Arenti {camera_name.replace('_', ' ').title()}"
        self._attr_unique_id = f"arenti_talk_{camera_name}"
        self._attr_state = MediaPlayerState.IDLE
        self._attr_volume_level = 0.5
        self._attr_supported_features = SUPPORT_ARENTI

    async def async_play_media(self, media_type: str, media_id: str, **kwargs) -> None:
        session = async_get_clientsession(self.hass)
        try:
            if media_id.startswith("http"):
                async with session.get(media_id) as resp:
                    data = await resp.read()
                    ct = resp.headers.get("Content-Type", "audio/mpeg")
                suffix = ".mp3" if "mp3" in ct else ".wav"
                form = {"file": (f"audio{suffix}", data, ct)}
                async with session.post(
                    f"{self._api_url}/talk/{self._camera_name}",
                    data={"file": data},
                ) as r:
                    pass
            else:
                async with session.post(
                    f"{self._api_url}/tts/{self._camera_name}",
                    json={"text": media_id, "lang": "fr"},
                ) as r:
                    pass
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
