#!/bin/bash

function run_scripts() {
    LOCK=/var/lib/cassandra/_init.done
    INIT_DIR=docker-entrypoint-initdb.d

    if [ -f "$LOCK" ]; then
        echo "==[INFO]: Initialisation already performed"
        exit 0
    fi

    echo "==[INFO]: Executing bash scripts found in $INIT_DIR"
    # execute scripts found in INIT_DIR
    cd $INIT_DIR
    for f in $(find . -type f -name "*.sh" -executable -print | sort); do
        echo "$0: sourcing $f"
        . "$f"
        echo "$0: $f executed."
    done

    # wait for cassandra to be ready and execute cql in background
    (
        while ! cqlsh -e 'describe cluster' >/dev/null 2>&1; do sleep 6; done
        echo "==[INFO]: Executing cql scripts found in $INIT_DIR"

        for f in $(find . -type f -name "*.cql" -print | sort); do
            echo "==[RUNING]: $f"
            cqlsh -f "$f"
            echo "==[DONE]: $f"
        done
        # mark things as initialized (in case /var/lib/cassandra was mapped to a local folder)
        touch $LOCK
    ) &
}

run_scripts
exec /docker-entrypoint.sh "$@"
