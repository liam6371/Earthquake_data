-- =================================================================
-- EARTHQUAKE DATA ANALYSIS SQL PROJECT
-- =================================================================
-- Business Context: Global earthquake monitoring and risk assessment
-- Data: ~1,800 earthquake records from 1995-2023 across two datasets
-- Skills Demonstrated: JOINs, GROUP BY, Aggregations, CTEs, Window Functions

-- =================================================================
-- 1. DATABASE SETUP AND TABLE CREATION
-- =================================================================

-- Create main earthquake events table (from earthquake_data.csv)
CREATE TABLE earthquake_events (
    event_id INT PRIMARY KEY IDENTITY(1,1),
    title VARCHAR(255),
    magnitude DECIMAL(3,1),
    date_time DATETIME,
    cdi INT,
    mmi INT,
    alert VARCHAR(10),
    tsunami TINYINT,
    sig INT,
    net VARCHAR(10),
    nst INT,
    dmin DECIMAL(8,3),
    gap DECIMAL(5,1),
    magType VARCHAR(10),
    depth DECIMAL(8,3),
    latitude DECIMAL(8,4),
    longitude DECIMAL(9,4),
    location VARCHAR(255),
    continent VARCHAR(50),
    country VARCHAR(100)
);

-- Create historical earthquake data table (from earthquake_19952023.csv)
CREATE TABLE earthquake_historical (
    event_id INT PRIMARY KEY IDENTITY(1,1),
    title VARCHAR(255),
    magnitude DECIMAL(3,1),
    date_time DATETIME,
    cdi INT,
    mmi INT,
    alert VARCHAR(10),
    tsunami TINYINT,
    sig INT,
    net VARCHAR(10),
    nst INT,
    dmin DECIMAL(8,3),
    gap DECIMAL(5,1),
    magType VARCHAR(10),
    depth DECIMAL(8,3),
    latitude DECIMAL(8,4),
    longitude DECIMAL(9,4),
    location VARCHAR(255),
    continent VARCHAR(50),
    country VARCHAR(100)
);

-- Create lookup table for monitoring networks
CREATE TABLE monitoring_networks (
    net_code VARCHAR(10) PRIMARY KEY,
    network_name VARCHAR(100),
    region VARCHAR(50)
);

-- Insert network data
INSERT INTO monitoring_networks VALUES 
('us', 'United States Geological Survey', 'Global'),
('at', 'National Institute of Geophysics and Volcanology', 'Europe'),
('pt', 'Portuguese Institute for Sea and Atmosphere', 'Europe'),
('ak', 'Alaska Earthquake Center', 'North America'),
('ci', 'California Institute of Technology', 'North America'),
('hv', 'Hawaiian Volcano Observatory', 'North America'),
('nc', 'Northern California Seismic Network', 'North America');

-- =================================================================
-- 2. DATA ANALYSIS QUERIES
-- =================================================================

-- Query 1: Global Earthquake Risk Assessment by Continent
-- Business Question: Which continents have the highest earthquake risk?
SELECT 
    e.continent,
    COUNT(*) as total_earthquakes,
    AVG(e.magnitude) as avg_magnitude,
    MAX(e.magnitude) as max_magnitude,
    SUM(CASE WHEN e.tsunami = 1 THEN 1 ELSE 0 END) as tsunami_events,
    SUM(CASE WHEN e.alert IN ('orange', 'red') THEN 1 ELSE 0 END) as high_risk_events,
    ROUND(AVG(e.depth), 2) as avg_depth_km
FROM (
    SELECT * FROM earthquake_events 
    UNION ALL 
    SELECT * FROM earthquake_historical
) e
WHERE e.continent IS NOT NULL
GROUP BY e.continent
ORDER BY avg_magnitude DESC;

-- Query 2: Network Performance Analysis with JOINs
-- Business Question: How do different monitoring networks compare in coverage and detection?
SELECT 
    mn.network_name,
    mn.region,
    COUNT(e.event_id) as events_detected,
    AVG(e.magnitude) as avg_magnitude_detected,
    AVG(e.nst) as avg_stations_used,
    AVG(e.gap) as avg_azimuthal_gap,
    ROUND(AVG(e.sig), 0) as avg_significance
FROM monitoring_networks mn
LEFT JOIN (
    SELECT * FROM earthquake_events 
    UNION ALL 
    SELECT * FROM earthquake_historical
) e ON mn.net_code = e.net
GROUP BY mn.network_name, mn.region
ORDER BY events_detected DESC;

-- Query 3: High-Risk Earthquake Analysis
-- Business Question: What are the characteristics of the most dangerous earthquakes?
WITH high_risk_earthquakes AS (
    SELECT 
        title,
        magnitude,
        depth,
        alert,
        tsunami,
        sig,
        continent,
        country,
        CASE 
            WHEN magnitude >= 8.0 THEN 'Major'
            WHEN magnitude >= 7.0 THEN 'Strong'
            ELSE 'Moderate'
        END as severity_category
    FROM (
        SELECT * FROM earthquake_events 
        UNION ALL 
        SELECT * FROM earthquake_historical
    ) 
    WHERE magnitude >= 7.0 OR alert IN ('orange', 'red') OR tsunami = 1
)
SELECT 
    severity_category,
    COUNT(*) as event_count,
    AVG(magnitude) as avg_magnitude,
    AVG(depth) as avg_depth,
    SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as tsunami_events,
    STRING_AGG(DISTINCT continent, ', ') as affected_continents
FROM high_risk_earthquakes
GROUP BY severity_category
ORDER BY avg_magnitude DESC;

-- Query 4: Geographic Hotspot Analysis
-- Business Question: Which countries experience the most frequent and severe earthquakes?
SELECT 
    country,
    continent,
    COUNT(*) as earthquake_count,
    ROUND(AVG(magnitude), 2) as avg_magnitude,
    MAX(magnitude) as max_magnitude,
    SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as tsunami_count,
    ROUND(AVG(sig), 0) as avg_significance,
    COUNT(CASE WHEN alert IN ('orange', 'red') THEN 1 END) as critical_alerts
FROM (
    SELECT * FROM earthquake_events 
    UNION ALL 
    SELECT * FROM earthquake_historical
) e
WHERE country IS NOT NULL
GROUP BY country, continent
HAVING COUNT(*) >= 5  -- Countries with at least 5 earthquakes
ORDER BY earthquake_count DESC, avg_magnitude DESC;

-- Query 5: Tsunami Risk Assessment
-- Business Question: What factors correlate with tsunami generation?
SELECT 
    CASE 
        WHEN magnitude < 7.0 THEN 'Under 7.0'
        WHEN magnitude < 7.5 THEN '7.0-7.4'
        WHEN magnitude < 8.0 THEN '7.5-7.9'
        ELSE '8.0+'
    END as magnitude_range,
    CASE 
        WHEN depth < 35 THEN 'Shallow (0-35km)'
        WHEN depth < 70 THEN 'Intermediate (35-70km)'
        ELSE 'Deep (70km+)'
    END as depth_category,
    COUNT(*) as total_earthquakes,
    SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as tsunami_events,
    ROUND(
        (SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2
    ) as tsunami_percentage
FROM (
    SELECT * FROM earthquake_events 
    UNION ALL 
    SELECT * FROM earthquake_historical
) e
WHERE magnitude IS NOT NULL AND depth IS NOT NULL
GROUP BY 
    CASE 
        WHEN magnitude < 7.0 THEN 'Under 7.0'
        WHEN magnitude < 7.5 THEN '7.0-7.4'
        WHEN magnitude < 8.0 THEN '7.5-7.9'
        ELSE '8.0+'
    END,
    CASE 
        WHEN depth < 35 THEN 'Shallow (0-35km)'
        WHEN depth < 70 THEN 'Intermediate (35-70km)'
        ELSE 'Deep (70km+)'
    END
ORDER BY magnitude_range, depth_category;

-- Query 6: Alert System Effectiveness Analysis
-- Business Question: How well does the alert system correlate with actual earthquake impact?
SELECT 
    alert,
    COUNT(*) as event_count,
    AVG(magnitude) as avg_magnitude,
    AVG(sig) as avg_significance,
    AVG(mmi) as avg_perceived_intensity,
    SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as tsunami_events,
    ROUND(AVG(depth), 2) as avg_depth
FROM (
    SELECT * FROM earthquake_events 
    UNION ALL 
    SELECT * FROM earthquake_historical
) e
WHERE alert IS NOT NULL
GROUP BY alert
ORDER BY 
    CASE alert 
        WHEN 'red' THEN 4 
        WHEN 'orange' THEN 3 
        WHEN 'yellow' THEN 2 
        WHEN 'green' THEN 1 
    END DESC;

-- Query 7: Advanced Analysis - Rolling Averages and Trends
-- Business Question: Are earthquake patterns changing over time?
WITH yearly_stats AS (
    SELECT 
        YEAR(CAST(date_time AS DATE)) as earthquake_year,
        COUNT(*) as annual_count,
        AVG(magnitude) as avg_magnitude,
        MAX(magnitude) as max_magnitude,
        SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as tsunami_count
    FROM (
        SELECT * FROM earthquake_events 
        UNION ALL 
        SELECT * FROM earthquake_historical
    ) e
    WHERE date_time IS NOT NULL 
    GROUP BY YEAR(CAST(date_time AS DATE))
)
SELECT 
    earthquake_year,
    annual_count,
    ROUND(avg_magnitude, 2) as avg_magnitude,
    max_magnitude,
    tsunami_count,
    AVG(annual_count) OVER (
        ORDER BY earthquake_year 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as three_year_avg_count,
    AVG(avg_magnitude) OVER (
        ORDER BY earthquake_year 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as three_year_avg_magnitude
FROM yearly_stats
ORDER BY earthquake_year;

-- =================================================================
-- 3. BUSINESS INSIGHTS SUMMARY QUERY
-- =================================================================

-- Executive Summary Query for Stakeholder Presentation
WITH summary_stats AS (
    SELECT 
        COUNT(*) as total_earthquakes,
        COUNT(DISTINCT continent) as continents_affected,
        COUNT(DISTINCT country) as countries_affected,
        AVG(magnitude) as global_avg_magnitude,
        MAX(magnitude) as strongest_earthquake,
        SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) as total_tsunamis,
        SUM(CASE WHEN alert IN ('orange', 'red') THEN 1 ELSE 0 END) as high_risk_events
    FROM (
        SELECT * FROM earthquake_events 
        UNION ALL 
        SELECT * FROM earthquake_historical
    )
)
SELECT 
    'Global Earthquake Analysis Summary' as report_title,
    total_earthquakes,
    continents_affected,
    countries_affected,
    ROUND(global_avg_magnitude, 2) as avg_magnitude,
    strongest_earthquake,
    total_tsunamis,
    ROUND((total_tsunamis * 100.0 / total_earthquakes), 1) as tsunami_percentage,
    high_risk_events,
    ROUND((high_risk_events * 100.0 / total_earthquakes), 1) as high_risk_percentage
FROM summary_stats;

-- =================================================================
-- 4. INTERVIEW TALKING POINTS
-- =================================================================

/*
KEY SQL SKILLS DEMONSTRATED:

1. **JOINs**: LEFT JOIN between monitoring_networks and earthquake data
2. **UNION**: Combining two earthquake datasets for comprehensive analysis
3. **GROUP BY & Aggregations**: COUNT, AVG, MAX, SUM across multiple dimensions
4. **CTEs**: Used for complex multi-step analysis and code organization
5. **Window Functions**: Rolling averages for trend analysis
6. **CASE Statements**: Data categorization and conditional logic
7. **String Functions**: STRING_AGG for concatenating results
8. **Date Functions**: YEAR extraction for temporal analysis
9. **Subqueries**: Complex filtering and data selection
10. **Data Types**: Proper handling of decimals, dates, and text

BUSINESS VALUE:
- Risk assessment for disaster preparedness
- Geographic hotspot identification
- Monitoring network performance evaluation
- Tsunami prediction factor analysis
- Alert system effectiveness measurement

INTERVIEW EXPLANATION:
"I analyzed global earthquake data to provide insights for disaster preparedness. 
The project combines two datasets using UNION, joins reference data for network analysis, 
and uses advanced SQL techniques like CTEs and window functions to identify risk patterns, 
geographic hotspots, and trending behaviors. This demonstrates my ability to work with 
real-world data and translate complex queries into actionable business insights."
*/