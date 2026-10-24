# Copyright 2026 Google LLC
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

# Control plane image for auto-optimizer.
#
# The control plane needs no tools; tools live in the sandbox image.

FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -buildvcs=false -ldflags="-s -w" \
      -o /ao ./cmd/ao

FROM gcr.io/distroless/static:nonroot
COPY --from=build /ao /ao

ENTRYPOINT ["/ao"]
CMD ["serve"]
