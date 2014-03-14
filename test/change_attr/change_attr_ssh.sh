#!/bin/bash
dir="$(cd "$(dirname "$0")"; echo $PWD)"
socat FD:$NODE_CHANNEL_FD "EXEC:ssh $1 $dir/change_attr_remote.py ${@:2}"
