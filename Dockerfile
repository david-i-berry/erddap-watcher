FROM python:3.9.13-slim

ENV TZ="Etc/UTC" \
    DEBIAN_FRONTEND="noninteractive" \
    DEBIAN_PACKAGES="cron bash vim git libffi-dev libeccodes0 python3-eccodes python3-cryptography libssl-dev libudunits2-0 python3-paho-mqtt python3-dateparser python3-tz python3-setuptools"

# We need latest version of BUFR tables, these are only available in bookworm release, add to sources
RUN echo 'deb http://deb.debian.org/debian bookworm main' >> /etc/apt/sources.list

RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until \
    && apt-get update -y \
    && apt-get install -y -t bookworm libeccodes-data \
    && apt-get install -y ${DEBIAN_PACKAGES} \
    && apt-get install -y python3 python3-pip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/csv2bufr/archive/refs/tags/v0.3.1.zip