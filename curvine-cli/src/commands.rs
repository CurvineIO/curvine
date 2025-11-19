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
    #[command(name = "fs")]
    Fs(FsCommand),

    #[command(name = "report")]
    Report(ReportCommand),

    /// Loading external files into Curvine
    #[command(name = "load")]
    Load(LoadCommand),

    /// Query loading task status
    #[command(name = "load-status")]
    LoadStatus(LoadStatusCommand),

    /// Cancel loading task
    #[command(name = "cancel-load")]
    CancelLoad(CancelLoadCommand),

    /// mount ufs to curvine
    #[command(name = "mount")]
    Mount(MountCommand),

    /// unmount ufs
    #[command(name = "umount")]
    UnMount(UnMountCommand),

    /// Node command
    #[command(name = "node")]
    Node(NodeCommand),

    /// Kubernetes deployment commands
    #[command(name = "k8s")]
    K8s(K8sCommand),

    /// show cli version
    #[command(name = "version")]
    Version,
}
