USE education_monitoring;

SELECT COUNT(*) AS total_events
FROM raw_events;

SELECT
    student_id,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY student_id
ORDER BY total_events DESC;

SELECT
    split(event, ',')[4] AS event_type,
    COUNT(*) AS event_count
FROM raw_events
GROUP BY split(event, ',')[4]
ORDER BY event_count DESC;

SELECT
    split(event, ',')[3] AS course_id,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY split(event, ',')[3]
ORDER BY total_events DESC;