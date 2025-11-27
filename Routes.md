# Vessels

The `vessels` route exposes endpoints to query current positions and position history for vessels (by MMSI).

- Base path: `/vessels`

**Subroutes**:
- `position`: Current positions and filters for multiple vessels
- `history`: Position history for a specific vessel (by MMSI)

**Top-level**: `GET /vessels`
- **Description**: Overview of available subroutes

---

**Vessels Position** (`/vessels/position`)
- `GET /vessels/position/` : Overview / help for the position endpoints

- `GET /vessels/position/all` : Returns current positions for all vessels with optional filters
	- **Query parameters**:
		- `limit` : Number of results (default: very high / effectively unlimited)
		- `minLatitude` / `minLat` : Minimum latitude filter
		- `maxLatitude` / `maxLat` : Maximum latitude filter
		- `minLongitude` / `minLon` : Minimum longitude filter
		- `maxLongitude` / `maxLon` : Maximum longitude filter
		- `shipType` / `vesselTypes` : Filter by vessel types (comma-separated list of integer values)
	- **Example**:
		- `GET /vessels/position/all?limit=50&minLatitude=10&maxLatitude=50`

- `GET /vessels/position/:mmsi` : (Not implemented yet)
	- **Response**: `501 Not Implemented` (endpoint under development)

**Response fields (from `/all`)**:
- `MMSI`, `ShipName`, `Timestamp`, `Latitude`, `Longitude`, `NavigationStatus`, `RateOfTurn`, `SpeedOverGround`, `CourseOverGround`, `TrueHeading`, `VesselType`

---

**Vessels History** (`/vessels/history`)
- `GET /vessels/history/` : Overview / help for history endpoints

- `GET /vessels/history/:mmsi` : Position history for a given MMSI
	- **Query parameters**:
		- `limit` : Number of results (default: 1000). Use `limit=no` for unlimited results.
		- `order` : Sort order: `asc` (default) or `desc`
		- `hour` : Filter from a specific hour today (0-23)
		- `day` : Filter from a specific day of the year (1-366)
		- `week` : Filter from a specific calendar week (1-53)
		- `month` : Filter from a specific month (1-12)
		- `year` : Filter by year (e.g. 2024). Implementation checks a limited range: 2000-2025
	- **Example**:
		- `GET /vessels/history/123456789?limit=50&order=desc&hour=14`

- `GET /vessels/history/:mmsi/count` : Count of position records for a specific MMSI
	- **Example**: `GET /vessels/history/123456789/count` → `{ mmsi: "123456789", count: 1234 }`

- `GET /vessels/history/:mmsi/latest` : Latest position record for a specific MMSI
	- **Example**: `GET /vessels/history/123456789/latest` → JSON object with the latest position fields

**Response fields (history / latest)**:
- `MMSI`, `NavigationStatus`, `RateOfTurn`, `SpeedOverGround`, `CourseOverGround`, `TrueHeading`, `Longitude`, `Latitude`, `SpecialManoeuvreIndicator`, `Timestamp`