{{ config(materialized='view') }}

SELECT
    award_level_desc,
    award_level_sort,
    cip_family,

    -- Totals
    SUM(total_completions) AS total_completions,
    SUM(total_men) AS total_men,
    SUM(total_women) AS total_women,

    -- Race/ethnicity counts
    SUM(aian_total) AS aian_total, -- AIAN — American Indian and Alaska Native
    SUM(asian_total) AS asian_total,
    SUM(black_total) AS black_total,
    SUM(hispanic_total) AS hispanic_total,
    SUM(nhpi_total)  AS nhpi_total, -- NHPI — Native Hawaiian and Pacific Islander
    SUM(white_total) AS white_total,
    SUM(two_or_more_total) AS two_or_more_total,
    SUM(unknown_total) AS unknown_total,
    SUM(nonresident_total) AS nonresident_total,
    SUM(students_of_color_total) AS students_of_color_total,

    -- Recalculated percentages from aggregated totals
    ROUND(SUM(black_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_black,
    ROUND(SUM(hispanic_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_hispanic,
    ROUND(SUM(white_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_white,
    ROUND(SUM(asian_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_asian,
    ROUND(SUM(aian_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_aian,
    ROUND(SUM(nhpi_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_nhpi,
    ROUND(SUM(two_or_more_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_two_or_more,
    ROUND(SUM(unknown_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_unknown,
    ROUND(SUM(nonresident_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_nonresident,
    ROUND(SUM(students_of_color_total)::numeric / NULLIF(SUM(total_completions), 0) * 100, 2) AS pct_students_of_color

FROM {{ ref('mart_completions_by_race') }}
GROUP BY
    award_level_desc,
    award_level_sort,
    cip_family
ORDER BY
    award_level_sort,
    cip_family