// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cmds::*;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run Curvine benchmarks
    #[command(name = "bench")]
    Bench(BenchCommand),

    #[command(name = "fs")]
    Fs(FsCommand),

    #[command(name = "report")]
    Report(ReportCommand),

    /// Loading external files into Curvine
    #[command(name = "load")]
    Load(LoadCommand),

    /// Exporting Curvine files to mounted UFS
    #[command(name = "export")]
    Export(ExportCommand),

    /// Query loading task status
    #[command(name = "load-status", hide = true)]
    LoadStatus(LoadStatusCommand),

    /// Query transfer task status
    #[command(name = "transfer-status", hide = true)]
    TransferStatus(LoadStatusCommand),

    /// Cancel loading task
    #[command(name = "cancel-load", hide = true)]
    CancelLoad(CancelLoadCommand),

    /// Cancel transfer task
    #[command(name = "cancel-transfer", hide = true)]
    CancelTransfer(CancelLoadCommand),

    /// Manage transfer jobs
    #[command(name = "transfer")]
    Transfer(TransferCommand),

    /// mount ufs to curvine
    #[command(name = "mount")]
    Mount(MountCommand),

    /// unmount ufs
    #[command(name = "umount")]
    UnMount(UnMountCommand),

    /// Node command
    #[command(name = "node")]
    Node(NodeCommand),

    /// show cli version
    #[command(name = "version")]
    Version,
}
