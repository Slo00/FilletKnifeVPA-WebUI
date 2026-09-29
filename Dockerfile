# Copyright The Kubernetes Authors. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# go.mod must point the VPA module at the GitHub fork, not at a local path
# (see README): the build context is this repository alone.
FROM --platform=$BUILDPLATFORM golang:1.26.5 AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY static static

ARG TARGETOS TARGETARCH
ARG BUILD_LD_FLAGS="-s"

# The page (static/) is embedded into the binary.
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH GOOS=$TARGETOS go build -ldflags="${BUILD_LD_FLAGS}" -o vpa-ui .

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /workspace/vpa-ui /vpa-ui

ENTRYPOINT ["/vpa-ui"]
