/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package lpx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func deploymentPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := newDeploymentV1ContractFixture(t).Message().Marshal()
	require.NoError(t, err)
	return payload
}

func TestGCSModelRegistrySnapshotUsesDeploymentFromModelExpress(t *testing.T) {
	t.Parallel()

	t.Log("Serve a valid revision-1 compiler manifest through Model Express")
	payload := deploymentPayload(t)
	shortCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, testCase := range []struct {
		registryURL string
		ref         string
		ctx         context.Context
	}{
		{registryURL: "gs://bucket/registry", ref: "model/build", ctx: t.Context()},
		{ref: "gs://bucket/registry/model/build", ctx: t.Context()},
		{registryURL: "gs://bucket/registry", ref: "model/build", ctx: shortCtx},
	} {
		t.Run(testCase.ref, func(t *testing.T) {
			t.Log("Create an independent client and registry for this reference form")
			client := &fakeModelServiceClient{
				files: map[string]*fakeModelFileStream{
					gbuildDeploymentV1CapnpFile: {chunks: modelFileChunks(gbuildDeploymentV1CapnpFile, string(payload))},
				},
			}
			registry, err := NewModelRegistry(testCase.registryURL, client)
			require.NoError(t, err)

			t.Log("Read and validate the manifest once, retaining the current registry locator")
			started := time.Now()
			build, err := normalizeRegistryFixtureBuild(testCase.ctx, registry, testCase.ref)
			require.NoError(t, err)
			require.Equal(t, "gs://bucket/registry/model/build", build.path)
			require.Len(t, build.partitions, 1)
			require.Empty(t, client.listRequests)
			require.Len(t, client.filesRequests, 1)
			require.Equal(t, []string{gbuildDeploymentV1CapnpFile}, client.filesRequests[0].GetFileSelector().GetPaths())

			t.Log("Bound both candidate reads to one acquisition deadline and release their contexts")
			require.Len(t, client.metadataContexts, 1)
			deadline, ok := client.metadataContexts[0].Deadline()
			require.True(t, ok)
			if parentDeadline, hasDeadline := testCase.ctx.Deadline(); hasDeadline {
				require.Equal(t, parentDeadline, deadline)
			} else {
				require.WithinRange(t, deadline, started.Add(30*time.Second), time.Now().Add(30*time.Second))
			}
			for _, rpcCtx := range client.metadataContexts {
				rpcDeadline, hasDeadline := rpcCtx.Deadline()
				require.True(t, hasDeadline)
				require.Equal(t, deadline, rpcDeadline)
				require.ErrorIs(t, rpcCtx.Err(), context.Canceled)
			}
			require.NoError(t, testCase.ctx.Err())
		})
	}
}

func TestAcquireBuildRejectsInvalidGCSContracts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		files   []string
		payload string
		wantErr error
		message string
	}{
		{
			name: "corrupt deployment", files: []string{gbuildDeploymentV1CapnpFile},
			payload: "invalid compiler bytes", wantErr: errInvalidBuildManifest, message: "parsing",
		},
		{
			name: "empty manifest exists", files: []string{gbuildDeploymentV1CapnpFile},
			wantErr: errInvalidBuildManifest, message: "parsing",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Log("Expose candidate streams through strict Model Express path selection")
			client := &fakeModelServiceClient{files: map[string]*fakeModelFileStream{}}
			for _, name := range test.files {
				client.files[name] = &fakeModelFileStream{chunks: modelFileChunks(name, test.payload)}
			}

			registry, err := NewModelRegistry("gs://bucket/registry", client)
			require.NoError(t, err)

			t.Log("Read the deployment contract and reject malformed bytes")
			snapshot, err := registry.AcquireBuild(t.Context(), "model/build")
			require.ErrorIs(t, err, test.wantErr)
			require.ErrorContains(t, err, test.message)
			require.Nil(t, snapshot)
			require.Empty(t, client.listRequests)
			require.Len(t, client.filesRequests, 1)
		})
	}
}

func TestAcquireBuildPreservesGCSReadFailures(t *testing.T) {
	t.Parallel()

	for _, readErr := range []error{
		status.Error(codes.NotFound, "manifest missing"),
		status.Error(codes.Unavailable, "object store unavailable"),
		status.Error(codes.PermissionDenied, "access denied"),
		status.Error(codes.DeadlineExceeded, "read deadline exceeded"),
		status.Error(codes.Canceled, "read canceled"),
		errors.New("transport failure"),
	} {
		t.Run(readErr.Error(), func(t *testing.T) {
			t.Log("Fail reading the deployment contract")
			client := &fakeModelServiceClient{files: map[string]*fakeModelFileStream{
				gbuildDeploymentV1CapnpFile: {chunks: modelFileChunks(gbuildDeploymentV1CapnpFile, "compiler bytes")},
			}}
			client.files[gbuildDeploymentV1CapnpFile] = &fakeModelFileStream{err: readErr}
			registry, err := NewModelRegistry("gs://bucket/registry", client)
			require.NoError(t, err)

			t.Log("Preserve the deployment read error")
			snapshot, err := registry.AcquireBuild(t.Context(), "model/build")
			require.ErrorIs(t, err, readErr)
			require.NotErrorIs(t, err, errInvalidBuildManifest)
			require.Nil(t, snapshot)
			require.Empty(t, client.listRequests)
		})
	}
}
