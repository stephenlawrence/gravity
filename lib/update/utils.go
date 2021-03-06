/*
Copyright 2018 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package update

import (
	appservice "github.com/gravitational/gravity/lib/app"
	"github.com/gravitational/gravity/lib/constants"
	"github.com/gravitational/gravity/lib/fsm"
	"github.com/gravitational/gravity/lib/loc"
	"github.com/gravitational/gravity/lib/schema"
	"github.com/gravitational/gravity/lib/storage"

	"github.com/gravitational/trace"
	"github.com/sirupsen/logrus"
)

// GetOperationPlan returns an up-to-date operation plan
func GetOperationPlan(b storage.Backend) (*storage.OperationPlan, error) {
	op, err := storage.GetLastOperation(b)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	plan, err := b.GetOperationPlan(op.SiteDomain, op.ID)
	if err != nil && !trace.IsNotFound(err) {
		return nil, trace.Wrap(err)
	}

	if plan == nil {
		return nil, trace.NotFound(
			"%q does not have a plan, use 'gravity plan --init' to initialize it", op.Type)
	}

	changelog, err := b.GetOperationPlanChangelog(op.SiteDomain, op.ID)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	plan = fsm.ResolvePlan(*plan, changelog)
	return plan, nil
}

// systemNeedsUpdate determines whether planet or teleport services need
// to be updated by comparing versions of respective packages in the
// installed and update application manifest
func systemNeedsUpdate(profile string, installed, update appservice.Application) (planetNeedsUpdate, teleportNeedsUpdate bool, err error) {
	installedProfile, err := installed.Manifest.NodeProfiles.ByName(profile)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	updateProfile, err := update.Manifest.NodeProfiles.ByName(profile)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	updateRuntimePackage, err := update.Manifest.RuntimePackage(*updateProfile)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	updateRuntimeVersion, err := updateRuntimePackage.SemVer()
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	installedRuntimePackage, err := getRuntimePackage(installed.Manifest, *installedProfile, schema.ServiceRoleMaster)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	installedRuntimeVersion, err := installedRuntimePackage.SemVer()
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	logrus.Debugf("Runtime installed: %v, runtime to update to: %v.",
		installedRuntimePackage, updateRuntimePackage)

	installedTeleportPackage, err := installed.Manifest.Dependencies.ByName(
		constants.TeleportPackage)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	installedTeleportVersion, err := installedTeleportPackage.SemVer()
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	updateTeleportPackage, err := update.Manifest.Dependencies.ByName(
		constants.TeleportPackage)
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	updateTeleportVersion, err := updateTeleportPackage.SemVer()
	if err != nil {
		return false, false, trace.Wrap(err)
	}

	logrus.Debugf("Teleport installed: %v, teleport update: %v.",
		installedTeleportPackage, updateTeleportPackage)

	return installedRuntimeVersion.LessThan(*updateRuntimeVersion),
		installedTeleportVersion.LessThan(*updateTeleportVersion), nil
}

func getRuntimePackage(manifest schema.Manifest, profile schema.NodeProfile, clusterRole schema.ServiceRole) (*loc.Locator, error) {
	runtimePackage, err := manifest.RuntimePackage(profile)
	if err != nil && !trace.IsNotFound(err) {
		return nil, trace.Wrap(err)
	}
	if err == nil {
		return runtimePackage, nil
	}
	// Look for legacy package
	packageName := loc.LegacyPlanetMaster.Name
	if clusterRole == schema.ServiceRoleNode {
		packageName = loc.LegacyPlanetNode.Name
	}
	runtimePackage, err = manifest.Dependencies.ByName(packageName)
	if err != nil {
		logrus.Warnf("Failed to find the legacy runtime package in manifest "+
			"for profile %v and cluster role %v: %v.", profile.Name, clusterRole, err)
		return nil, trace.NotFound("runtime package for profile %v "+
			"(cluster role %v) not found in manifest",
			profile.Name, clusterRole)
	}
	return runtimePackage, nil
}
