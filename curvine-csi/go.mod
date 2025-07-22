module github.com/curvineio/curvine-csi

go 1.22.0

require (
	github.com/container-storage-interface/spec v1.11.0
	google.golang.org/grpc v1.60.1
	k8s.io/klog v1.0.0
)

require (
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace google.golang.org/grpc v1.73.0 => google.golang.org/grpc v1.60.1

replace golang.org/x/net v0.38.0 => golang.org/x/net v0.20.0

replace golang.org/x/sys v0.31.0 => golang.org/x/sys v0.16.0

replace golang.org/x/text v0.23.0 => golang.org/x/text v0.14.0

replace google.golang.org/protobuf v1.36.6 => google.golang.org/protobuf v1.31.0

replace google.golang.org/genproto/googleapis/rpc v0.0.0-20250324211829-b45e905df463 => google.golang.org/genproto/googleapis/rpc v0.0.0-20231120223509-83a465c0220f

replace github.com/container-storage-interface/spec v1.11.0 => github.com/container-storage-interface/spec v1.8.0
