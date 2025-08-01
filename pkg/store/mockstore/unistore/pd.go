// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/asaskevich/govalidator"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	us "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
	"google.golang.org/grpc"
)

var _ pd.Client = new(pdClient)

type pdClient struct {
	*us.MockPD
	pd.ResourceManagerClient

	globalConfig      map[string]string
	externalTimestamp atomic.Uint64

	// After using PD http client, we should impl mock PD service discovery
	// which needs PD server HTTP address.
	addrs []string

	keyspaceMeta *keyspacepb.KeyspaceMeta
}

func newPDClient(pd *us.MockPD, addrs []string, keyspaceMeta *keyspacepb.KeyspaceMeta) *pdClient {
	keyspaceID := constants.NullKeyspaceID
	if keyspaceMeta != nil {
		keyspaceID = keyspaceMeta.GetId()
	}
	return &pdClient{
		MockPD:                pd,
		ResourceManagerClient: infosync.NewMockResourceManagerClient(keyspaceID),
		globalConfig:          make(map[string]string),
		addrs:                 addrs,
		keyspaceMeta:          keyspaceMeta,
	}
}

func (c *pdClient) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]pd.GlobalConfigItem, int64, error) {
	ret := make([]pd.GlobalConfigItem, len(names))
	for i, name := range names {
		if r, ok := c.globalConfig["/global/config/"+name]; ok {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Value: r, EventType: pdpb.EventType_PUT}
		} else {
			ret[i] = pd.GlobalConfigItem{Name: "/global/config/" + name, Value: ""}
		}
	}
	return ret, 0, nil
}

func (c *pdClient) StoreGlobalConfig(ctx context.Context, configPath string, items []pd.GlobalConfigItem) error {
	for _, item := range items {
		c.globalConfig["/global/config/"+item.Name] = item.Value
	}
	return nil
}

func (c *pdClient) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []pd.GlobalConfigItem, error) {
	globalConfigWatcherCh := make(chan []pd.GlobalConfigItem, 16)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		for range 10 {
			for k, v := range c.globalConfig {
				globalConfigWatcherCh <- []pd.GlobalConfigItem{{Name: k, Value: v}}
			}
		}
	}()
	return globalConfigWatcherCh, nil
}

func (c *pdClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *pdClient) GetTSAsync(ctx context.Context) tso.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

func (c *pdClient) GetLocalTSAsync(ctx context.Context, dcLocation string) tso.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

func (c *pdClient) GetServiceDiscovery() sd.ServiceDiscovery {
	return NewMockPDServiceDiscovery(c.addrs)
}

var (
	_ sd.ServiceDiscovery = (*mockPDServiceDiscovery)(nil)
	_ sd.ServiceClient    = (*mockPDServiceClient)(nil)
)

type mockPDServiceClient struct {
	addr string
}

func newMockPDServiceClient(addr string) sd.ServiceClient {
	if !strings.HasPrefix(addr, "http") {
		addr = fmt.Sprintf("%s://%s", "http", addr)
	}
	return &mockPDServiceClient{addr: addr}
}

func (c *mockPDServiceClient) GetAddress() string {
	return c.addr
}

func (c *mockPDServiceClient) GetURL() string {
	return c.addr
}

func (c *mockPDServiceClient) GetClientConn() *grpc.ClientConn {
	return nil
}

func (c *mockPDServiceClient) BuildGRPCTargetContext(ctx context.Context, _ bool) context.Context {
	return ctx
}

func (c *mockPDServiceClient) Available() bool {
	return true
}

func (c *mockPDServiceClient) NeedRetry(*pdpb.Error, error) bool {
	return false
}

func (c *mockPDServiceClient) IsConnectedToLeader() bool {
	return true
}

type mockPDServiceDiscovery struct {
	addrs []string
	clis  []sd.ServiceClient
}

// NewMockPDServiceDiscovery returns a mock PD ServiceDiscovery
func NewMockPDServiceDiscovery(addrs []string) sd.ServiceDiscovery {
	addresses := make([]string, 0)
	clis := make([]sd.ServiceClient, 0)
	for _, addr := range addrs {
		if check := govalidator.IsURL(addr); !check {
			continue
		}
		addresses = append(addresses, addr)
		clis = append(clis, newMockPDServiceClient(addr))
	}
	return &mockPDServiceDiscovery{addrs: addresses, clis: clis}
}

func (c *mockPDServiceDiscovery) Init() error {
	return nil
}

func (c *mockPDServiceDiscovery) Close() {}

func (c *mockPDServiceDiscovery) GetClusterID() uint64 { return 0 }

func (c *mockPDServiceDiscovery) GetKeyspaceID() uint32 { return 0 }

func (c *mockPDServiceDiscovery) SetKeyspaceID(uint32) {}

func (c *mockPDServiceDiscovery) GetKeyspaceGroupID() uint32 { return 0 }

func (c *mockPDServiceDiscovery) GetServiceURLs() []string {
	return c.addrs
}

func (c *mockPDServiceDiscovery) GetServingEndpointClientConn() *grpc.ClientConn { return nil }

func (c *mockPDServiceDiscovery) GetClientConns() *sync.Map { return nil }

func (c *mockPDServiceDiscovery) GetServingURL() string { return "" }

func (c *mockPDServiceDiscovery) GetBackupURLs() []string { return nil }

func (c *mockPDServiceDiscovery) GetServiceClient() sd.ServiceClient {
	if len(c.clis) > 0 {
		return c.clis[0]
	}
	return nil
}

func (c *mockPDServiceDiscovery) GetServiceClientByKind(sd.APIKind) sd.ServiceClient {
	return c.GetServiceClient()
}

func (c *mockPDServiceDiscovery) GetAllServiceClients() []sd.ServiceClient {
	return c.clis
}

func (c *mockPDServiceDiscovery) GetOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	return nil, nil
}

func (c *mockPDServiceDiscovery) ScheduleCheckMemberChanged() {}

func (c *mockPDServiceDiscovery) CheckMemberChanged() error { return nil }

func (c *mockPDServiceDiscovery) AddServingAddrSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServiceAddrsSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServingURLSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddServiceURLsSwitchedCallback(callbacks ...func()) {}

func (c *mockPDServiceDiscovery) AddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {
}

func (c *mockPDServiceDiscovery) AddMembersChangedCallback(func()) {}

func (c *mockPDServiceDiscovery) ExecAndAddLeaderSwitchedCallback(sd.LeaderSwitchedCallbackFunc) {
}

type mockTSFuture struct {
	pdc  *pdClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetLeaderURL() string { return "mockpd" }

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *pdClient) GetAllMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	return nil, nil
}

func (c *pdClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *pdClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...opt.GetRegionOption) (*router.Region, error) {
	return nil, nil
}

func (c *pdClient) UpdateOption(option opt.DynamicOption, value any) error {
	return nil
}

func (c *pdClient) GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

// LoadKeyspace loads and returns target keyspace's metadata.
func (c *pdClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	if c.keyspaceMeta == nil {
		return nil, errors.New("keyspace is unsupported")
	}
	if c.keyspaceMeta.Name != name {
		return nil, errors.New(pdpb.ErrorType_ENTRY_NOT_FOUND.String())
	}
	return c.keyspaceMeta, nil
}

// WatchKeyspaces watches keyspace meta changes.
// It returns a stream of slices of keyspace metadata.
// The first message in stream contains all current keyspaceMeta,
// all subsequent messages contains new put events for all keyspaces.
func (c *pdClient) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (c *pdClient) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error {
	p, l, err := c.GetTS(ctx)
	if err != nil {
		return err
	}

	currentTSO := oracle.ComposeTS(p, l)
	if newTimestamp > currentTSO {
		return errors.New("external timestamp is greater than global tso")
	}
	for {
		externalTimestamp := c.externalTimestamp.Load()
		if externalTimestamp > newTimestamp {
			return errors.New("cannot decrease the external timestamp")
		} else if externalTimestamp == newTimestamp {
			return nil
		}

		if c.externalTimestamp.CompareAndSwap(externalTimestamp, newTimestamp) {
			return nil
		}
	}
}

func (c *pdClient) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	return c.externalTimestamp.Load(), nil
}

func (c *pdClient) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) tso.TSFuture {
	return nil
}

func (c *pdClient) GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) tso.TSFuture {
	return nil
}

func (c *pdClient) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return nil, nil
}

func (c *pdClient) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return nil, nil
}

func (c *pdClient) GetMinTS(ctx context.Context) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

func (c *pdClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	panic("unimplemented")
}

func (c *pdClient) WithCallerComponent(component caller.Component) pd.Client {
	return c
}
