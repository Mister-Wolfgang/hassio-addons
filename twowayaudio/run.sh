#!/usr/bin/with-contenv bashio

export GO2RTC_API=$(bashio::config 'go2rtc_api')
export VOLUME=$(bashio::config 'volume')
export TTS_LANGUAGE=$(bashio::config 'tts_language')

exec uvicorn main:app --host 0.0.0.0 --port 8081
