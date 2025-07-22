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


package main

import (
	"flag"
	"fmt"
	"os"

	"k8s.io/klog"

	"github.com/curvineio/curvine-csi/pkg/csi"
)


var (
	endpoint   = flag.String("endpoint", "unix://tmp/csi.sock", "CSI Endpoint")
	version    = flag.Bool("version", false, "Print the version and exit.")
	nodeID     = flag.String("nodeid", "", "Node ID")
	configPath = flag.String("config", "", "Path to config file")
)

func main() {
	flag.Parse()
	if *version {
		info, err := csi.GetVersionJSON()
		if err != nil {
			klog.Fatalln(err)
		}
		fmt.Println(info)
		os.Exit(0)
	}
	if *nodeID == "" {
		klog.Fatalln("nodeID must be provided")
	}

	drv := csi.NewDriver(*endpoint, *nodeID, *configPath)
	if err := drv.Run(); err != nil {
		klog.Fatalln(err)
	}
}
