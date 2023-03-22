/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

const (
	// ManagerName is name of manager.
	ManagerName = "manager"

	// SchedulerName is name of scheduler.
	SchedulerName = "scheduler"

	// DfdaemonName is name of dfdaemon.
	DfdaemonName = "dfdaemon"

	// DaemonName is daemon name of dfdaemon.
	DaemonName = "daemon"

	// DfgetName is dfget name of dfdaemon.
	DfgetName = "dfget"

	// DfcacheName is dfcache name of dfdaemon.
	DfcacheName = "dfcache"

	// DfstoreName is dfstore name of dfdaemon.
	DfstoreName = "dfstore"
)

const (
	// SourcePattern is back-to-source download pattern.
	SourcePattern = "source"

	// SeedPeerPattern is seed peer download pattern.
	SeedPeerPattern = "seed-peer"

	// P2PPattern is p2p download pattern.
	P2PPattern = "p2p"
)

const (
	// MetricsNamespace is namespace of metrics.
	MetricsNamespace = "dragonfly"

	// ManagerMetricsName is name of manager metrics.
	ManagerMetricsName = "manager"

	// SchedulerMetricsName is name of scheduelr metrics.
	SchedulerMetricsName = "scheduler"

	// DfdaemonMetricsName is name of dfdaemon metrics.
	DfdaemonMetricsName = "dfdaemon"
)

const (
	// AffinitySeparator is separator of affinity.
	AffinitySeparator = "|"
)
