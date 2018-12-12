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

package helm

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gravitational/gravity/lib/loc"
	"github.com/gravitational/gravity/lib/pack"

	"k8s.io/helm/pkg/chartutil"
	"k8s.io/helm/pkg/provenance"
	"k8s.io/helm/pkg/repo"

	"github.com/ghodss/yaml"
	"github.com/gravitational/trace"
)

// Repository defines a Helm repository interface.
type Repository interface {
	FetchChart(chartName, chartVersion string) (io.ReadCloser, error)
	GetIndexFile() (io.ReadCloser, error)
	PutChart(chartName, chartVersion string, data io.Reader) error
	DeleteChart(chartName, chartVersion string) error
}

type Config struct {
	Packages pack.PackageService
}

type clusterRepository struct {
	Config
	sync.Mutex
}

func NewRepository(config Config) (*clusterRepository, error) {
	return &clusterRepository{
		Config: config,
	}, nil
}

func (r *clusterRepository) FetchChart(chartName, chartVersion string) (io.ReadCloser, error) {
	// name, version, err := parseChartFilename(chartFilename)
	// if err != nil {
	// 	return nil, trace.Wrap(err)
	// }
	locator, err := loc.NewLocator("charts", chartName, chartVersion)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	_, reader, err := r.Packages.ReadPackage(*locator)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return reader, nil
}

func (r *clusterRepository) GetIndexFile() (io.ReadCloser, error) {
	_, reader, err := r.Packages.ReadPackage(indexLoc)
	return reader, trace.Wrap(err)
}

func (r *clusterRepository) PutChart(name, version string, data io.Reader) error {
	locator, err := loc.NewLocator("charts", name, version)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = r.Packages.CreatePackage(*locator, data)
	if err != nil {
		return trace.Wrap(err)
	}
	err = r.addToIndex(*locator)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (r *clusterRepository) removeFromIndex(chartName, chartVersion string) error {
	r.Lock()
	defer r.Unlock()
	indexFile, err := r.getIndexFile()
	if err != nil {
		return trace.Wrap(err)
	}
	for name, versions := range indexFile.Entries {
		if name == chartName {
			for i, version := range versions {
				if version.Version == chartVersion {
					indexFile.Entries[name] = append(
						versions[:i], versions[i+1:]...)
					break
				}
			}
		}
	}
	data, err := yaml.Marshal(indexFile)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = r.Packages.UpsertPackage(indexLoc, bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (r *clusterRepository) addToIndex(locator loc.Locator) error {
	r.Lock()
	defer r.Unlock()
	_, reader, err := r.Packages.ReadPackage(locator)
	if err != nil {
		return trace.Wrap(err)
	}
	defer reader.Close()
	chart, err := chartutil.LoadArchive(reader)
	if err != nil {
		return trace.Wrap(err)
	}
	digest, err := r.digest(locator)
	if err != nil {
		return trace.Wrap(err)
	}
	indexFile, err := r.getIndexFile()
	if err != nil && !trace.IsNotFound(err) {
		return trace.Wrap(err)
	}
	if trace.IsNotFound(err) {
		indexFile = repo.NewIndexFile()
	}
	if indexFile.Has(chart.Metadata.Name, chart.Metadata.Version) {
		return trace.AlreadyExists("chart %v:%v already exists",
			chart.Metadata.Name, chart.Metadata.Version)
	}
	indexFile.Add(chart.Metadata, fmt.Sprintf("%v-%v.tgz",
		chart.Metadata.Name, chart.Metadata.Version), filepath.Join(
		r.Packages.PortalURL(), "charts"), digest)
	indexFile.SortEntries()
	data, err := yaml.Marshal(indexFile)
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = r.Packages.UpsertPackage(indexLoc, bytes.NewReader(data))
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func (r *clusterRepository) digest(locator loc.Locator) (string, error) {
	_, reader, err := r.Packages.ReadPackage(locator)
	if err != nil {
		return "", trace.Wrap(err)
	}
	defer reader.Close()
	digest, err := provenance.Digest(reader)
	if err != nil {
		return "", trace.Wrap(err)
	}
	return digest, nil
}

func (r *clusterRepository) getIndexFile() (*repo.IndexFile, error) {
	_, reader, err := r.Packages.ReadPackage(indexLoc)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	var indexFile *repo.IndexFile
	err = yaml.Unmarshal(data, indexFile)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return indexFile, nil
}

func (r *clusterRepository) DeleteChart(chartName, chartVersion string) error {
	locator, err := loc.NewLocator("charts", chartName, chartVersion)
	if err != nil {
		return trace.Wrap(err)
	}
	err = r.removeFromIndex(chartName, chartVersion)
	if err != nil {
		return trace.Wrap(err)
	}
	err = r.Packages.DeletePackage(*locator)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}

var indexLoc = loc.Locator{
	Repository: "charts",
	Name:       "index",
	Version:    "0.0.1",
}

func parseChartFilename(filename string) (name, version string, err error) {
	parts := strings.Split(strings.TrimSuffix(filename, ".tgz"), "-")
	if len(parts) != 2 {
		return "", "", trace.BadParameter("bad chart filename: %v", filename)
	}
	return parts[0], parts[1], nil
}
