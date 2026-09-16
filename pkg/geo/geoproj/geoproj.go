// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoproj

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// This is the pure-Go PROJ wrapper. The C dependency on PROJ has been removed
// from this build of CockroachDB: coordinate reference system
// transformations return a descriptive runtime error instead.

const projDisabledMsg = "coordinate transformation unavailable: this CockroachDB build does not link the PROJ library"

func projUnavailable() error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "%s", projDisabledMsg)
}

// GetProjMetadata returns metadata about the given projection. In a no-cgo
// build it always fails.
func GetProjMetadata(b geoprojbase.Proj4Text) (bool, *geographiclib.Spheroid, error) {
	return false, nil, projUnavailable()
}

// Project projects the given xCoords, yCoords and zCoords from one
// coordinate system to another using proj4text. In a no-cgo build it always
// fails.
func Project(
	from geoprojbase.Proj4Text,
	to geoprojbase.Proj4Text,
	xCoords []float64,
	yCoords []float64,
	zCoords []float64,
) error {
	return projUnavailable()
}
