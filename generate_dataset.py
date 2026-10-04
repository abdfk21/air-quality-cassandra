"""
generate_dataset.py
-------------------
Generates a realistic, synthetic time-series air quality telemetry dataset
for 5 major metropolitan cities across 6-12 months.

Includes:
- Diurnal (morning/evening rush hour) cycles
- Meteorological variations
- Realistic episodic pollution spikes (thermal inversions, crop burning, dust storms)
- UN SDG 11 (Sustainable Cities and Communities) indicator metrics (PM2.5, PM10, NO2, CO, SO2, AQI)
"""

import argparse
import datetime
import math
import random
import pandas as pd
import numpy as np


# City profiles with baseline ambient concentrations (mean, std dev)
CITY_PROFILES = {
    "Delhi": {
        "pm2_5_base": 110.0,
        "pm2_5_std": 35.0,
        "stations": ["DEL_ANAND_VIHAR", "DEL_ITO", "DEL_PUNJABI_BAGH"],
        "spike_prob": 0.08,     # High frequency of winter / episodic spikes
        "spike_mult": (2.2, 4.0),
    },
    "Mumbai": {
        "pm2_5_base": 55.0,
        "pm2_5_std": 18.0,
        "stations": ["MUM_BKC", "MUM_COLABA", "MUM_ANDHERI"],
        "spike_prob": 0.03,     # Coastal winds moderate pollution
        "spike_mult": (1.8, 2.8),
    },
    "Bengaluru": {
        "pm2_5_base": 42.0,
        "pm2_5_std": 14.0,
        "stations": ["BLR_WHITEFIELD", "BLR_SILK_BOARD", "BLR_BTM_LAYOUT"],
        "spike_prob": 0.02,     # Generally cleaner plateau climate
        "spike_mult": (1.6, 2.5),
    },
    "Hyderabad": {
        "pm2_5_base": 65.0,
        "pm2_5_std": 22.0,
        "stations": ["HYD_HITEC_CITY", "HYD_CHARMINAR"],
        "spike_prob": 0.035,
        "spike_mult": (1.7, 3.0),
    },
    "Kolkata": {
        "pm2_5_base": 85.0,
        "pm2_5_std": 28.0,
        "stations": ["KOL_PARK_STREET", "KOL_HOWRAH"],
        "spike_prob": 0.05,
        "spike_mult": (1.9, 3.2),
    },
}


def calculate_aqi_sub_index(pm25: float) -> int:
    """
    Computes standard PM2.5 sub-index based on CPCB (India) / EPA breakpoints:
      0-30   -> Good (0-50)
      31-60  -> Satisfactory (51-100)
      61-90  -> Moderate (101-200)
      91-120 -> Poor (201-300)
      121-250-> Very Poor (301-400)
      250+   -> Severe / Hazardous (401-500+)
    """
    if pm25 <= 30:
        return int(pm25 * (50 / 30))
    elif pm25 <= 60:
        return int(50 + (pm25 - 30) * (50 / 30))
    elif pm25 <= 90:
        return int(100 + (pm25 - 60) * (100 / 30))
    elif pm25 <= 120:
        return int(200 + (pm25 - 90) * (100 / 30))
    elif pm25 <= 250:
        return int(300 + (pm25 - 120) * (100 / 130))
    else:
        # Severe / hazardous spike
        return min(500, int(400 + (pm25 - 250) * (100 / 150)))


def generate_time_series_data(days: int = 180, interval_hours: int = 2, seed: int = 42) -> pd.DataFrame:
    """Generates synthetic hourly/bi-hourly air quality sensor observations."""
    random.seed(seed)
    np.random.seed(seed)

    end_date = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - datetime.timedelta(days=days)

    timestamps = []
    current_time = start_date
    while current_time <= end_date:
        timestamps.append(current_time)
        current_time += datetime.timedelta(hours=interval_hours)

    records = []

    for city, config in CITY_PROFILES.items():
        base_pm = config["pm2_5_base"]
        std_pm = config["pm2_5_std"]
        stations = config["stations"]
        spike_prob = config["spike_prob"]

        # Track persistent multi-day episodic events
        in_episode = False
        episode_remaining = 0
        episode_mult = 1.0

        for t in timestamps:
            hour = t.hour
            day_of_year = t.timetuple().tm_yday

            # 1. Diurnal traffic cycle: Peaks at 09:00 and 20:00, trough at 14:00
            diurnal_factor = 1.0 + 0.35 * math.sin((hour - 4) * math.pi / 12)

            # 2. Seasonal cycle: Pollution rises in autumn/winter months (days 280-365 and 1-60)
            winter_factor = 1.0 + 0.40 * math.cos((day_of_year - 15) * 2 * math.pi / 365)

            # 3. Episodic spike trigger (e.g., smog, dust storm, fire, stagnant weather)
            if not in_episode:
                if random.random() < spike_prob:
                    in_episode = True
                    episode_remaining = random.randint(3, 10)  # 6-20 hours duration
                    episode_mult = random.uniform(*config["spike_mult"])
            else:
                episode_remaining -= 1
                if episode_remaining <= 0:
                    in_episode = False
                    episode_mult = 1.0

            for station in stations:
                # Add local micro-climate variance per sensor station
                station_noise = random.gauss(0, std_pm * 0.25)
                raw_pm25 = (base_pm + station_noise) * diurnal_factor * winter_factor

                if in_episode:
                    raw_pm25 *= episode_mult

                # Apply non-negative physical bounds
                pm2_5 = round(max(5.0, raw_pm25 + random.gauss(0, 5.0)), 2)
                
                # PM10 is strongly correlated with PM2.5 (ratio ~1.5x - 2.2x plus road dust)
                pm10 = round(pm2_5 * random.uniform(1.45, 1.95) + random.uniform(5.0, 20.0), 2)
                
                # Nitrogen Dioxide (NO2) - heavily linked to vehicular traffic rush hours
                no2_base = 25.0 if city != "Delhi" else 45.0
                no2 = round(max(5.0, (no2_base + random.gauss(0, 8.0)) * diurnal_factor * (1.5 if in_episode else 1.0)), 2)

                # Carbon Monoxide (CO in mg/m³) - incomplete combustion
                co = round(max(0.1, (0.8 + 0.005 * pm2_5) + random.gauss(0, 0.2)), 2)

                # Sulfur Dioxide (SO2 in µg/m³) - industrial emissions
                so2 = round(max(2.0, (12.0 + 0.03 * pm2_5) + random.gauss(0, 3.0)), 2)

                # Calculated AQI
                aqi = calculate_aqi_sub_index(pm2_5)

                # Spike Alert trigger condition
                spike_alert = bool(aqi >= 300 or pm2_5 >= 250.0)

                records.append({
                    "city": city,
                    "recorded_at": t.strftime("%Y-%m-%d %H:%M:%S"),
                    "station_id": station,
                    "pm2_5": pm2_5,
                    "pm10": pm10,
                    "no2": no2,
                    "co": co,
                    "so2": so2,
                    "aqi": aqi,
                    "spike_alert": spike_alert,
                })

    df = pd.DataFrame(records)
    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic air quality time-series telemetry.")
    parser.add_argument("--days", type=int, default=180, help="Number of historical days to simulate (default: 180)")
    parser.add_argument("--interval", type=int, default=2, help="Sampling interval in hours (default: 2)")
    parser.add_argument("--output", type=str, default="air_quality_data.csv", help="Output CSV path")
    args = parser.parse_args()

    print(f"[*] Simulating air quality telemetry across 5 cities for {args.days} days ({args.interval}-hour intervals)...")
    df = generate_time_series_data(days=args.days, interval_hours=args.interval)
    
    df.to_csv(args.output, index=False)
    print(f"[+] Dataset generated successfully: {args.output}")
    print(f"    - Total records: {len(df):,}")
    print(f"    - Date range: {df['recorded_at'].min()} to {df['recorded_at'].max()}")
    print(f"    - Total spike alert events: {df['spike_alert'].sum():,} ({df['spike_alert'].mean() * 100:.2f}%)")
    print("\nRecords per city:")
    for city, count in df["city"].value_counts().items():
        spikes = df[df["city"] == city]["spike_alert"].sum()
        print(f"    * {city:<12}: {count:,} records | {spikes:,} spike alerts")


if __name__ == "__main__":
    main()
