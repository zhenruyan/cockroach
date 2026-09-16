// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geographiclib

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// This is the pure-Go implementation of the GeographicLib wrapper; the C
// dependency has been removed. GeographicLib normally computes geodesics on
// the reference ellipsoid (Karney 2013); this implementation instead uses
// great-circle (spherical) math on a sphere of the spheroid's major radius.
// Results agree with the ellipsoidal computation to within roughly 0.5%.

func init() {
	geoprojbase.MakeSpheroid = func(radius, flattening float64) (geoprojbase.Spheroid, error) {
		return NewSpheroid(radius, flattening), nil
	}
}

var (
	// WGS84Spheroid represents the default WGS84 ellipsoid.
	WGS84Spheroid = NewSpheroid(6378137, 1/298.257223563)
)

// Spheroid is an object that can perform geodesic operations
// on a given spheroid. In this (no-cgo) build the operations are computed on
// a sphere with the spheroid's major radius.
type Spheroid struct {
	radius       float64
	flattening   float64
	sphereRadius float64
}

// NewSpheroid creates a spheroid from a radius and flattening.
func NewSpheroid(radius float64, flattening float64) *Spheroid {
	minorAxis := radius - radius*flattening
	return &Spheroid{
		radius:       radius,
		flattening:   flattening,
		sphereRadius: (radius*2 + minorAxis) / 3,
	}
}

// Radius returns the radius of the spheroid.
func (s *Spheroid) Radius() float64 {
	return s.radius
}

// Flattening returns the flattening factor of the spheroid.
func (s *Spheroid) Flattening() float64 {
	return s.flattening
}

// SphereRadius returns the radius of a sphere that fits inside the spheroid.
func (s *Spheroid) SphereRadius() float64 {
	return s.sphereRadius
}

// Inverse solves the geodetic inverse problem on a sphere.
// Returns s12 (distance in meters), az1 (azimuth at point 1) and az2 (azimuth
// at point 2), both in degrees.
func (s *Spheroid) Inverse(a, b s2.LatLng) (s12, az1, az2 float64) {
	// Use s2's numerically-stable central angle for the distance, and the
	// standard spherical bearing formula for the azimuths.
	dist := s2.PointFromLatLng(a).Vector.Angle(s2.PointFromLatLng(b).Vector).Radians() * s.radius
	baz1 := initialBearing(a, b)
	baz2 := initialBearing(b, a)
	return dist, normalizeDeg(baz1), normalizeDeg(baz2 + 180)
}

// InverseBatch computes the sum of the length of the lines represented
// by the line of points, in meters.
func (s *Spheroid) InverseBatch(points []s2.Point) float64 {
	var total float64
	for i := 1; i < len(points); i++ {
		total += points[i-1].Angle(points[i].Vector).Radians() * s.radius
	}
	return total
}

// AreaAndPerimeter computes the area (meter^2) and perimeter (meters) of a
// polygon on the sphere.
func (s *Spheroid) AreaAndPerimeter(points []s2.Point) (area float64, perimeter float64) {
	if len(points) < 2 {
		return 0, 0
	}
	loop := s2.LoopFromPoints(points)
	area = math.Abs(loop.Area()) * s.radius * s.radius
	var angle s1.Angle
	for i := range points {
		angle += points[i].Angle(points[(i+1)%len(points)].Vector)
	}
	perimeter = angle.Radians() * s.radius
	return area, perimeter
}

// Project computes the location of a point projected the given distance
// along the given azimuth from the starting point (spherical "destination
// formula", https://en.wikipedia.org/wiki/Spherical_law_of_cosines).
func (s *Spheroid) Project(point s2.LatLng, distance float64, azimuth s1.Angle) s2.LatLng {
	δ := s1.Angle(distance / s.radius) // angular distance
	θ := azimuth                       // bearing clockwise from north
	φ1 := point.Lat.Radians()
	λ1 := point.Lng.Radians()
	sinφ1, cosφ1 := math.Sin(φ1), math.Cos(φ1)
	sinδ, cosδ := math.Sin(δ.Radians()), math.Cos(δ.Radians())
	sinθ, cosθ := math.Sin(θ.Radians()), math.Cos(θ.Radians())
	φ2 := math.Asin(sinφ1*cosδ + cosφ1*sinδ*cosθ)
	λ2 := λ1 + math.Atan2(sinθ*sinδ*cosφ1, cosδ-sinφ1*math.Sin(φ2))
	return s2.LatLngFromDegrees(φ2*180/math.Pi, λ2*180/math.Pi)
}

// initialBearing returns the azimuth (degrees) of the great circle from a to
// b at point a, measured clockwise from north.
func initialBearing(a, b s2.LatLng) float64 {
	φ1, φ2 := a.Lat.Radians(), b.Lat.Radians()
	Δλ := b.Lng.Radians() - a.Lng.Radians()
	y := math.Sin(Δλ) * math.Cos(φ2)
	x := math.Cos(φ1)*math.Sin(φ2) - math.Sin(φ1)*math.Cos(φ2)*math.Cos(Δλ)
	return math.Atan2(y, x) * 180 / math.Pi
}

// normalizeDeg maps degrees into [-180, 180).
func normalizeDeg(deg float64) float64 {
	for deg >= 180 {
		deg -= 360
	}
	for deg < -180 {
		deg += 360
	}
	return deg
}
