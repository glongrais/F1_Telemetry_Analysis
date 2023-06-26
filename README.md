# F1_Telemetry_Analysis

## Requirements

- Python 3.8 or later
- [FastF1](https://docs.fastf1.dev/#) 3.0.4

## Projects
### Drivers Gap

```json
{
  "<driver_number>": {
    "<entry_index>": {
      "Date": <integer>,
      "RPM": <integer>,
      "Speed": <integer>,
      "nGear": <integer>,
      "Throttle": <integer>,
      "Brake": <boolean>,
      "DRS": <integer>,
      "Source": <string>,
      "Time": <integer>,
      "SessionTime": <integer>,
      "DriverAhead": <string>,
      "DistanceToDriverAhead": <null or integer>
    },
    ...
  },
  ...
}
```