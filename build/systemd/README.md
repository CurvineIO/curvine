# Running Curvine with systemd

This directory holds systemd unit templates for bare-metal deployments. The
units run the master, the worker and FUSE mounts under systemd, which restarts
them after a crash, starts them at boot and sends their console output to the
journal.

systemd is optional. `bin/curvine-master.sh start`, `bin/restart-all.sh` and
the other scripts keep working as before. Do not manage the same service with
both: the scripts track their processes in `<service>.pid` files, while the
units run them in the foreground with no PID file.

| Unit | Runs |
| ---- | ---- |
| `curvine-master.service` | `bin/curvine-master.sh run` |
| `curvine-worker.service` | `bin/curvine-worker.sh run` |
| `curvine-fuse@.service` | `bin/curvine-fuse.sh run --mnt-path <path>`, one instance per mount point |

The `run` action of these scripts sources `conf/curvine-env.sh`, like `start`
does, and then `exec`s the binary in the foreground. systemd therefore tracks
`curvine-server` or `curvine-fuse` itself and sends `SIGTERM` to it directly.

## Install

Run the helper from the Curvine installation directory (`CURVINE_HOME`) as
root:

```bash
bin/curvine-systemd.sh install                    # services run as root
bin/curvine-systemd.sh install --user curvine     # or as a dedicated account
```

The helper renders the templates with the absolute `CURVINE_HOME` path and the
service account, writes them to `/etc/systemd/system` and runs
`systemctl daemon-reload`. It does not enable or start anything. Pass
`--unit-dir DIR` to write the units somewhere else, for example when building
a package; `systemctl` is not called then.

A non-root account must be able to write the configured log, metadata,
journal and data directories. Mounting FUSE as a non-root user also needs
`fusermount` and `user_allow_other` in `/etc/fuse.conf`, so FUSE units usually
run as root.

Then enable the services this host runs:

```bash
systemctl enable --now curvine-master.service
systemctl enable --now curvine-worker.service
systemctl enable --now "$(systemd-escape --path --template=curvine-fuse@.service /curvine-fuse)"
```

`enable` starts the units at boot (`WantedBy=multi-user.target`); `--now`
also starts them immediately.

## FUSE mounts

The instance name of `curvine-fuse@.service` is the escaped mount path, so
`/curvine-fuse` becomes `curvine-fuse@curvine\x2dfuse.service` and
`/mnt/curvine/data` becomes `curvine-fuse@mnt-curvine-data.service`. Each
mount point is an independent unit with its own process, so several mounts on
one host do not share a PID file.

Pass extra `curvine-fuse` options through `CURVINE_FUSE_OPTS` in a drop-in.
When a host has more than one mount, give each one its own metrics web port
(`--web-port`, 9003 by default):

```bash
systemctl edit curvine-fuse@mnt-curvine-data.service
```

```ini
[Service]
Environment="CURVINE_FUSE_OPTS=--web-port 9013 --conf /etc/curvine/fuse-data.toml"
```

`--mnt-path` comes from the instance name and must not be repeated there.
The unit pins `--mnt-number 1`: one unit manages one mount, and `run` rejects
any other `--mnt-number`, because the cleanup below only knows the mount path
itself and not the `mnt-0`, `mnt-1` submounts. Options are split on whitespace.

If `curvine-fuse` crashes or is killed, the kernel keeps a dead mount that
answers with `Transport endpoint is not connected`. The unit detaches that
mount with `umount -l` after every stop so that the automatic restart can
mount the path again. The cleanup only runs for a path that passed the
startup checks, so a path that was already mounted when the unit started is
refused and left untouched. Applications still holding files open on the dead
mount have to reopen them.

Hot reload (`curvine-fuse.sh reload`, `SIGUSR1`) is not supported for units
and `systemctl reload` is rejected. On `SIGUSR1` the running process hands its
mount to a successor process and exits, but systemd treats the exit of the
main process as the end of the service and cleans up its processes and the
mount. Use `systemctl restart`, which unmounts and mounts again.

## Ordering and restarts

- The worker is ordered after the master, and FUSE mounts after both, when
  those units run on the same host. The ordering uses `After=` without
  `Requires=`, so a worker-only or client-only host does not start a master.
  A service that starts before the master is reachable keeps retrying its
  connection as usual.
- `Restart=on-failure` with `RestartSec=5s` restarts a service that exits
  with an error or is killed by a signal other than `SIGTERM`, `SIGINT`,
  `SIGHUP` or `SIGPIPE`, for example by `kill -9` or the OOM killer. A clean
  stop does not restart it. systemd gives up after 5 starts within 5 minutes
  (`StartLimitBurst=5`, `StartLimitIntervalSec=300`); run
  `systemctl reset-failed <unit>` after fixing the cause.
- `systemctl stop` sends `SIGTERM` and waits up to 15 seconds
  (`TimeoutStopSec=15s`) for the graceful shutdown before `SIGKILL`, about
  the same window that `bin/launch-process.sh stop` uses.

## Logs

Console output goes to the journal:

```bash
journalctl -u curvine-master.service -f
journalctl -u 'curvine-fuse@*' --since today
```

The services still write their own log files as configured by the `log`
settings in `conf/curvine-cluster.toml`, with the rotation configured there.
Relative paths in that file, such as `log_dir = "logs"`, resolve against
`CURVINE_HOME`, the working directory of every unit.

## Customizing

Keep local changes in drop-ins (`systemctl edit <unit>`) so that reinstalling
the templates does not overwrite them. For example:

```ini
[Service]
# Use another configuration file
Environment=CURVINE_CONF_FILE=/etc/curvine/curvine-cluster.toml
# Resource limits
LimitNOFILE=2097152
MemoryMax=64G
OOMScoreAdjust=-500
# Allow a longer graceful shutdown
TimeoutStopSec=60s
```

Services started by systemd do not inherit the environment of a login shell.
Set what the binaries need in the drop-in, for example `LD_LIBRARY_PATH` for
the JindoSDK native libraries of OSS-HDFS mounts, or `HADOOP_HOME` and
`HADOOP_CONF_DIR` for HDFS mounts that use the JNI client. A drop-in for
`curvine-fuse@.service` applies to every FUSE instance.

Variables set this way take precedence over the defaults in
`conf/curvine-env.sh` for the variables that file only defaults, such as
`CURVINE_CONF_FILE` and the `CURVINE_*_HOSTNAME` variables.

## Uninstall

```bash
bin/curvine-systemd.sh uninstall
```

This stops and disables the Curvine units, including every
`curvine-fuse@` instance, removes the unit files and reloads systemd.
Drop-in directories created by `systemctl edit` are left in place.
