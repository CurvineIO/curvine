FROM ubuntu:24.04

ARG GOPROXY

# 更换为清华大学TUNA镜像源并安装Go和FUSE3运行时依赖
RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list && \
    apt-get update && apt-get install -y \
    wget \
    tar \
    make \
    build-essential \
    fuse3 \
    libfuse3-3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 安装Go 1.23
RUN wget -O go1.23.linux-amd64.tar.gz https://go.dev/dl/go1.23.4.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.23.linux-amd64.tar.gz && \
    rm go1.23.linux-amd64.tar.gz

# 设置Go环境变量
ENV PATH=/usr/local/go/bin:$PATH
ENV GOPATH=/go
ENV GOROOT=/usr/local/go

WORKDIR /curvine
COPY . .
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org}

RUN make build && \
    mkdir -p /opt/curvine/bin && \
    mkdir -p /opt/curvine/conf && \
    cp -r curvine/bin/* /opt/curvine/bin && \
    cp -r curvine/conf/* /opt/curvine/conf && \
    cp bin/csi /opt/curvine/bin/ && \
    chmod u+x /opt/curvine/bin/csi && \
    chmod u+x /opt/curvine/bin/cv && \
    chmod u+x /opt/curvine/bin/curvine-fuse

WORKDIR /opt/curvine
#ENTRYPOINT ["/opt/curvine/bin/csi"]
