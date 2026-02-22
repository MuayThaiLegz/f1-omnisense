/**
 * Track geometry utilities — converts normalised 0-1 positions to real lat/lng
 * by interpolating along a GeoJSON LineString coordinate array.
 */

export interface LatLng {
  lng: number;
  lat: number;
}

/** Haversine distance between two [lng, lat] GeoJSON coordinates (metres). */
function haversineDistance(a: [number, number], b: [number, number]): number {
  const R = 6_371_000;
  const dLat = ((b[1] - a[1]) * Math.PI) / 180;
  const dLng = ((b[0] - a[0]) * Math.PI) / 180;
  const sinLat = Math.sin(dLat / 2);
  const sinLng = Math.sin(dLng / 2);
  const h =
    sinLat * sinLat +
    Math.cos((a[1] * Math.PI) / 180) * Math.cos((b[1] * Math.PI) / 180) * sinLng * sinLng;
  return R * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

/** Build cumulative arc-length array along a LineString. Element 0 = 0. */
export function buildCumulativeDistances(coords: [number, number][]): number[] {
  const d = new Float64Array(coords.length);
  for (let i = 1; i < coords.length; i++) {
    d[i] = d[i - 1] + haversineDistance(coords[i - 1], coords[i]);
  }
  return Array.from(d);
}

/** Given a normalised position t ∈ [0,1], return the interpolated lat/lng. */
export function interpolatePosition(
  coords: [number, number][],
  cumDist: number[],
  t: number,
): LatLng {
  const total = cumDist[cumDist.length - 1];
  const target = Math.max(0, Math.min(1, t)) * total;

  // Binary search for the segment containing target distance
  let lo = 0;
  let hi = cumDist.length - 1;
  while (lo < hi - 1) {
    const mid = (lo + hi) >> 1;
    if (cumDist[mid] <= target) lo = mid;
    else hi = mid;
  }

  const segLen = cumDist[hi] - cumDist[lo];
  const frac = segLen > 0 ? (target - cumDist[lo]) / segLen : 0;

  return {
    lng: coords[lo][0] + frac * (coords[hi][0] - coords[lo][0]),
    lat: coords[lo][1] + frac * (coords[hi][1] - coords[lo][1]),
  };
}

/**
 * Extract a sub-linestring between two normalised positions.
 * Handles wrap-around where startT > endT (zone crosses start/finish).
 */
export function extractSubLine(
  coords: [number, number][],
  cumDist: number[],
  startT: number,
  endT: number,
  numSamples = 20,
): [number, number][] {
  const pts: [number, number][] = [];

  if (startT <= endT) {
    // Normal segment
    for (let i = 0; i <= numSamples; i++) {
      const t = startT + (i / numSamples) * (endT - startT);
      const p = interpolatePosition(coords, cumDist, t);
      pts.push([p.lng, p.lat]);
    }
  } else {
    // Wrap-around: startT → 1.0 then 0.0 → endT
    const firstHalf = Math.ceil(numSamples * (1 - startT) / (1 - startT + endT));
    const secondHalf = numSamples - firstHalf;
    for (let i = 0; i <= firstHalf; i++) {
      const t = startT + (i / firstHalf) * (1 - startT);
      const p = interpolatePosition(coords, cumDist, Math.min(t, 0.999));
      pts.push([p.lng, p.lat]);
    }
    for (let i = 1; i <= secondHalf; i++) {
      const t = (i / secondHalf) * endT;
      const p = interpolatePosition(coords, cumDist, t);
      pts.push([p.lng, p.lat]);
    }
  }

  return pts;
}

/**
 * Split a coordinate array into 3 equal-length sectors.
 * Returns 3 arrays of [lng, lat] coordinates (overlapping at boundaries for continuity).
 */
export function splitIntoSectors(
  coords: [number, number][],
): [[number, number][], [number, number][], [number, number][]] {
  const n = coords.length;
  const s1End = Math.floor(n / 3);
  const s2End = Math.floor((2 * n) / 3);

  return [
    coords.slice(0, s1End + 1),
    coords.slice(s1End, s2End + 1),
    coords.slice(s2End),
  ];
}

/** Compute curvature at each point (angle change in radians). Returns array of length coords.length. */
export function computeCurvatures(coords: [number, number][]): number[] {
  const curvatures = new Array(coords.length).fill(0);
  for (let i = 1; i < coords.length - 1; i++) {
    const dx1 = coords[i][0] - coords[i - 1][0];
    const dy1 = coords[i][1] - coords[i - 1][1];
    const dx2 = coords[i + 1][0] - coords[i][0];
    const dy2 = coords[i + 1][1] - coords[i][1];
    const angle1 = Math.atan2(dy1, dx1);
    const angle2 = Math.atan2(dy2, dx2);
    let diff = Math.abs(angle2 - angle1);
    if (diff > Math.PI) diff = 2 * Math.PI - diff;
    curvatures[i] = diff;
  }
  return curvatures;
}

/**
 * Build an array of cumulative "line-progress" fractions and associated colours
 * for use with MapLibre's line-gradient expression. Maps curvature to green→yellow→red.
 */
export function buildSpeedGradientStops(
  coords: [number, number][],
  cumDist: number[],
): [number, string][] {
  const curvatures = computeCurvatures(coords);
  const total = cumDist[cumDist.length - 1];
  const stops: [number, string][] = [];

  // Smooth curvatures over a window
  const window = 5;
  const smoothed = curvatures.map((_, i) => {
    let sum = 0, count = 0;
    for (let j = Math.max(0, i - window); j <= Math.min(curvatures.length - 1, i + window); j++) {
      sum += curvatures[j];
      count++;
    }
    return sum / count;
  });

  const maxCurv = Math.max(...smoothed, 0.001);

  for (let i = 0; i < coords.length; i += 3) {
    const progress = total > 0 ? cumDist[i] / total : 0;
    const norm = Math.min(smoothed[i] / maxCurv, 1);

    // Green (fast/straight) → Yellow → Red (slow/tight)
    const r = Math.round(norm < 0.5 ? norm * 2 * 255 : 255);
    const g = Math.round(norm < 0.5 ? 255 : (1 - (norm - 0.5) * 2) * 255);
    const color = `rgb(${r},${g},0)`;

    stops.push([progress, color]);
  }

  return stops;
}

/** Compute bounding box from coordinates → [west, south, east, north]. */
export function getBounds(coords: [number, number][]): [number, number, number, number] {
  let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
  for (const [lng, lat] of coords) {
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  }
  return [minLng, minLat, maxLng, maxLat];
}
