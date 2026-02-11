SELECT
    '/' || substr(
        replace(url, 'https://docs.snowflake.com', ''),
        2,
        instr(replace(url, 'https://docs.snowflake.com', ''), '/', 2) - 2
    ) AS path_segment,
    COUNT(*) AS frequency
FROM CANDIDATE_SK_DOCS_MASTER
WHERE url LIKE 'https://docs.snowflake.com/%'
GROUP BY path_segment
ORDER BY frequency DESC, path_segment ASC
LIMIT 10;
