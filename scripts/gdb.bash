#!/bin/bash


prep_term()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term' TERM INT
}

handle_term()
{
    if [ "${term_child_pid}" ]; then
	#        kill -TERM "${term_child_pid}" 2>/dev/null
	gdb -x gdbcommands --pid "${term_child_pid}"
    else
        term_kill_needed="yes"
    fi
}

wait_term()
{
    term_child_pid=$!
    if [ "${term_kill_needed}" ]; then
	#        kill -TERM "${term_child_pid}" 2>/dev/null 
	gdb -x gdbcommands --pid "${term_child_pid}"
    fi
    wait ${term_child_pid} 2>/dev/null
    trap - TERM INT
    wait ${term_child_pid} 2>/dev/null
}

# EXAMPLE USAGE
prep_term
~/TileDB/build/tiledb/test/tiledb_unit -d yes &
wait_term
