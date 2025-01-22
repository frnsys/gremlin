//! For standardizing times by timezone.
//!
//! Adapted from [`automatic-timezoned`](https://crates.io/crates/automatic-timezoned),
//! which unfortunately is unavailable as a library.

use std::sync::LazyLock;

use chrono::{DateTime, TimeZone};
pub use chrono_tz::Tz;
use csv::{Reader, ReaderBuilder};
use geo::{point, prelude::HaversineDistance, Point};
use time::OffsetDateTime;

use super::error::DataResult;

const DEFAULT_ZONES: &str = include_str!("../../assets/zone1970.tab");
static ZONES: LazyLock<ZoneInfo> = LazyLock::new(ZoneInfo::default);

pub fn get_zone(coords: (f64, f64)) -> Tz {
    let (zone, _) = ZONES.find_closest_zone(coords.0, coords.1);
    zone
}

pub fn to_local(dt: &OffsetDateTime, zone: &Tz) -> OffsetDateTime {
    let ns = dt.unix_timestamp_nanos();
    let ndt = DateTime::from_timestamp_nanos(ns as i64);
    let local = zone.from_utc_datetime(&ndt.naive_utc());
    OffsetDateTime::from_unix_timestamp_nanos(
        local
            .timestamp_nanos_opt()
            .expect("We have a valid unix timestamp") as i128,
    )
    .expect("We have a valid unix timestamp")
}

/// Describes a timezone with its name and location.
#[derive(Debug)]
struct Zone {
    pub coordinates: Point,
    pub timezone: Tz,
}

/// Holds the IANA Time Zone Database
#[derive(Debug)]
struct ZoneInfo {
    pub zones: Vec<Zone>,
}
impl ZoneInfo {
    fn from_reader<R: std::io::Read>(mut reader: Reader<R>) -> DataResult<ZoneInfo> {
        let mut zones = vec![];
        while let Some(result) = reader.records().next() {
            let record = result?;
            let coordinates = parse_coordinates(&record[1])?;
            let timezone_name: String = record[2].parse()?;
            let zone = Zone {
                coordinates,
                timezone: timezone_name.parse().expect("A valid timezone name"),
            };

            zones.push(zone);
        }

        Ok(ZoneInfo { zones })
    }

    /// Calculate the distance between a given location and each zone and return the closest.
    pub fn find_closest_zone(&self, lat: f64, lon: f64) -> (Tz, f64) {
        let location = point!(x: lon, y: lat);
        let mut timezone: Tz = "UTC".parse().expect("This is a valid timezone");
        let mut distance = f64::MAX;

        for z in &self.zones {
            // Haversine over Vincenty/Karney's geodesic,
            // because it is fast and accuracy is not critical
            let d = location.haversine_distance(&z.coordinates);
            if distance > d {
                timezone = z.timezone;
                distance = d;
            }
        }

        (timezone, distance)
    }
}
impl Default for ZoneInfo {
    fn default() -> Self {
        let rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .comment(Some(b'#'))
            .flexible(true)
            .from_reader(DEFAULT_ZONES.as_bytes());
        Self::from_reader(rdr).unwrap()
    }
}

/// Parse a longitude or latitude ISO-6709 string into `f64`.
fn parse_coordinate_value(valstr: &str, separator: usize) -> DataResult<f64> {
    let whole: f64 = valstr[..separator].parse()?;
    let fractionstr: &str = &valstr[separator..];
    let fraction: f64 = fractionstr.parse()?;
    let value: f64 = if whole >= 0.0 {
        whole + fraction / f64::powf(10.0, fractionstr.len() as f64)
    } else {
        whole - fraction / f64::powf(10.0, fractionstr.len() as f64)
    };

    Ok(value)
}

/// Parse coordinates from ISO-6709 string to `geo::Point`.
fn parse_coordinates(coordinates: &str) -> DataResult<Point> {
    let mut i = 1;

    for c in coordinates.chars().skip(1) {
        if c == '-' || c == '+' {
            break;
        };
        i += 1;
    }

    let lat = parse_coordinate_value(&coordinates[..i], 3)?; // ±DDMMSS
    let lon = parse_coordinate_value(&coordinates[i..], 4)?; // ±DDDMMSS

    Ok(point!(x: lon, y: lat))
}
