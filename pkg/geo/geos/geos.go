// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geos

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// This is the pure-Go GEOS wrapper. The C dependency on GEOS has been removed
// from this build of CockroachDB: every operation that would be performed
// by the GEOS library returns a descriptive runtime error instead.

const geosDisabledMsg = "geometry operation unavailable: this CockroachDB build does not link the GEOS library"

func geosUnavailable() error {
	return pgerror.Newf(pgcode.FeatureNotSupported, "%s", geosDisabledMsg)
}

// EnsureInitErrorDisplay is used to control the error message displayed by
// EnsureInit.
type EnsureInitErrorDisplay int

const (
	// EnsureInitErrorDisplayPrivate displays the full error message, including
	// path info. It is intended for log messages.
	EnsureInitErrorDisplayPrivate EnsureInitErrorDisplay = iota
	// EnsureInitErrorDisplayPublic displays a redacted error message, excluding
	// path info. It is intended for errors to display for the client.
	EnsureInitErrorDisplayPublic
)

// PreparedGeometry is a handle to a prepared geometry. Prepared geometries
// are only (mass) used for intersections.
type PreparedGeometry *struct{}

// Error represents an error thrown within GEOS.
type Error struct {
	msg string
}

// Error implements the error interface.
func (err *Error) Error() string {
	return err.msg
}

// BufferParamsJoinStyle maps to the GEOSBufJoinStyles enum in geos_c.h.in.
type BufferParamsJoinStyle int

// These should be kept in sync with the geos_c.h.in corresponding enum definition.
const (
	BufferParamsJoinStyleRound = 1
	BufferParamsJoinStyleMitre = 2
	BufferParamsJoinStyleBevel = 3
)

// BufferParamsEndCapStyle maps to the GEOSBufCapStyles enum in geos_c.h.in.
type BufferParamsEndCapStyle int

// These should be kept in sync with the geos_c.h.in corresponding enum definition.
const (
	BufferParamsEndCapStyleRound  = 1
	BufferParamsEndCapStyleFlat   = 2
	BufferParamsEndCapStyleSquare = 3
)

// BufferParams are parameters to provide into the GEOS buffer function.
type BufferParams struct {
	JoinStyle        BufferParamsJoinStyle
	EndCapStyle      BufferParamsEndCapStyle
	SingleSided      bool
	QuadrantSegments int
	MitreLimit       float64
}

// EnsureInit is unavailable without cgo.
func EnsureInit(errDisplay EnsureInitErrorDisplay, flagLibraryDirectoryValue string) (string, error) {
	return "", geosUnavailable()
}

// Version is unavailable without cgo.
func Version() (string, error) {
	return "", geosUnavailable()
}

// Buffer is unavailable without cgo.
func Buffer(ewkb geopb.EWKB, params BufferParams, distance float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Area is unavailable without cgo.
func Area(ewkb geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// Boundary is unavailable without cgo.
func Boundary(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Difference is unavailable without cgo.
func Difference(ewkb1 geopb.EWKB, ewkb2 geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Length is unavailable without cgo.
func Length(ewkb geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// Normalize is unavailable without cgo.
func Normalize(a geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// LineMerge is unavailable without cgo.
func LineMerge(a geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// IsSimple is unavailable without cgo.
func IsSimple(ewkb geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Centroid is unavailable without cgo.
func Centroid(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// MinimumBoundingCircle is unavailable without cgo.
func MinimumBoundingCircle(ewkb geopb.EWKB) (geopb.EWKB, geopb.EWKB, float64, error) {
	return nil, nil, 0, geosUnavailable()
}

// ConvexHull is unavailable without cgo.
func ConvexHull(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Simplify is unavailable without cgo.
func Simplify(ewkb geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// TopologyPreserveSimplify is unavailable without cgo.
func TopologyPreserveSimplify(ewkb geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// PointOnSurface is unavailable without cgo.
func PointOnSurface(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Intersection is unavailable without cgo.
func Intersection(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// UnaryUnion is unavailable without cgo.
func UnaryUnion(a geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Union is unavailable without cgo.
func Union(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// SymDifference is unavailable without cgo.
func SymDifference(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// InterpolateLine is unavailable without cgo.
func InterpolateLine(ewkb geopb.EWKB, distance float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// MinDistance is unavailable without cgo.
func MinDistance(a geopb.EWKB, b geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// MinimumClearance is unavailable without cgo.
func MinimumClearance(ewkb geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// MinimumClearanceLine is unavailable without cgo.
func MinimumClearanceLine(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// ClipByRect is unavailable without cgo.
func ClipByRect(ewkb geopb.EWKB, xMin float64, yMin float64, xMax float64, yMax float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// PrepareGeometry is unavailable without cgo.
func PrepareGeometry(a geopb.EWKB) (PreparedGeometry, error) {
	return nil, geosUnavailable()
}

// PreparedGeomDestroy is unavailable without cgo.
func PreparedGeomDestroy(a PreparedGeometry) {}

// Covers is unavailable without cgo.
func Covers(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// CoveredBy is unavailable without cgo.
func CoveredBy(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Contains is unavailable without cgo.
func Contains(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Crosses is unavailable without cgo.
func Crosses(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Disjoint is unavailable without cgo.
func Disjoint(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Equals is unavailable without cgo.
func Equals(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// PreparedIntersects is unavailable without cgo.
func PreparedIntersects(a PreparedGeometry, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Intersects is unavailable without cgo.
func Intersects(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Overlaps is unavailable without cgo.
func Overlaps(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Touches is unavailable without cgo.
func Touches(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// Within is unavailable without cgo.
func Within(a geopb.EWKB, b geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// FrechetDistance is unavailable without cgo.
func FrechetDistance(a, b geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// FrechetDistanceDensify is unavailable without cgo.
func FrechetDistanceDensify(a, b geopb.EWKB, densifyFrac float64) (float64, error) {
	return 0, geosUnavailable()
}

// HausdorffDistance is unavailable without cgo.
func HausdorffDistance(a, b geopb.EWKB) (float64, error) {
	return 0, geosUnavailable()
}

// HausdorffDistanceDensify is unavailable without cgo.
func HausdorffDistanceDensify(a, b geopb.EWKB, densifyFrac float64) (float64, error) {
	return 0, geosUnavailable()
}

// EqualsExact is unavailable without cgo.
func EqualsExact(lhs, rhs geopb.EWKB, epsilon float64) (bool, error) {
	return false, geosUnavailable()
}

// Relate is unavailable without cgo.
func Relate(a geopb.EWKB, b geopb.EWKB) (string, error) {
	return "", geosUnavailable()
}

// RelateBoundaryNodeRule is unavailable without cgo.
func RelateBoundaryNodeRule(a geopb.EWKB, b geopb.EWKB, bnr int) (string, error) {
	return "", geosUnavailable()
}

// RelatePattern is unavailable without cgo.
func RelatePattern(a geopb.EWKB, b geopb.EWKB, pattern string) (bool, error) {
	return false, geosUnavailable()
}

// IsValid is unavailable without cgo.
func IsValid(ewkb geopb.EWKB) (bool, error) {
	return false, geosUnavailable()
}

// IsValidReason is unavailable without cgo.
func IsValidReason(ewkb geopb.EWKB) (string, error) {
	return "", geosUnavailable()
}

// IsValidDetail is unavailable without cgo.
func IsValidDetail(ewkb geopb.EWKB, flags int) (bool, string, geopb.EWKB, error) {
	return false, "", nil, geosUnavailable()
}

// MakeValid is unavailable without cgo.
func MakeValid(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// SharedPaths is unavailable without cgo.
func SharedPaths(a geopb.EWKB, b geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Node is unavailable without cgo.
func Node(a geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// VoronoiDiagram is unavailable without cgo.
func VoronoiDiagram(a, env geopb.EWKB, tolerance float64, onlyEdges bool) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// MinimumRotatedRectangle is unavailable without cgo.
func MinimumRotatedRectangle(ewkb geopb.EWKB) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}

// Snap is unavailable without cgo.
func Snap(input, target geopb.EWKB, tolerance float64) (geopb.EWKB, error) {
	return nil, geosUnavailable()
}
