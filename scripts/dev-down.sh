#!/usr/bin/env bash
pkill -f 'flowforge-.*-SNAPSHOT.jar' && echo "stopped" || echo "nothing running"
