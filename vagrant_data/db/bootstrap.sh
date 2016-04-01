#!/bin/bash

DB_NAME=pagecloud

sudo locale-gen en_US.UTF-8
dpkg-reconfigure locales

export LANGUAGE=en_US.UTF-8
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

#createdb -U postgres $DB_NAME
