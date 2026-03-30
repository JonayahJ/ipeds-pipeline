--
-- PostgreSQL database dump
--

\restrict j1qLltK2hg5ohZuqB6l7KKkpOHOl37HDr0UjXxwKtvGcd6Ug2aflCL3oSPTeuj5

-- Dumped from database version 16.13 (Homebrew)
-- Dumped by pg_dump version 16.13 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: mart_completions_by_race; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.mart_completions_by_race AS
 WITH completions AS (
         SELECT stg_completions_2024.unitid,
            stg_completions_2024.cipcode,
            stg_completions_2024.majornum,
            stg_completions_2024.awlevel,
            stg_completions_2024.total_completions,
            stg_completions_2024.total_men,
            stg_completions_2024.total_women,
            stg_completions_2024.aian_total,
            stg_completions_2024.asian_total,
            stg_completions_2024.black_total,
            stg_completions_2024.hispanic_total,
            stg_completions_2024.nhpi_total,
            stg_completions_2024.white_total,
            stg_completions_2024.two_or_more_total,
            stg_completions_2024.unknown_total,
            stg_completions_2024.nonresident_total
           FROM public.stg_completions_2024
          WHERE (stg_completions_2024.cipcode <> '99'::text)
        ), institutions AS (
         SELECT stg_institutions_2024.unitid,
            stg_institutions_2024.institution_name,
            stg_institutions_2024.address,
            stg_institutions_2024.city,
            stg_institutions_2024.state,
            stg_institutions_2024.zip,
            stg_institutions_2024.website,
            stg_institutions_2024.county,
            stg_institutions_2024.latitude,
            stg_institutions_2024.longitude,
            stg_institutions_2024.hbcu,
            stg_institutions_2024.tribal,
            stg_institutions_2024.hospital,
            stg_institutions_2024.medical,
            stg_institutions_2024.degree_granting,
            stg_institutions_2024.carnegie_classification,
            stg_institutions_2024.institution_size,
            stg_institutions_2024.control_type,
            stg_institutions_2024.institution_level,
            stg_institutions_2024.sector_desc,
            stg_institutions_2024.institution_size_desc
           FROM public.stg_institutions_2024
        ), joined AS (
         SELECT i.unitid,
            i.institution_name,
            i.city,
            i.state,
            i.county,
            i.control_type,
            i.institution_level,
            i.sector_desc,
            i.institution_size_desc,
            (i.hbcu = 1) AS is_hbcu,
            (i.tribal = 1) AS is_tribal,
            (i.hospital = 1) AS is_hospital,
            (i.medical = 1) AS is_medical,
            (i.state = ANY (ARRAY['PR'::text, 'GU'::text, 'VI'::text, 'MP'::text, 'AS'::text])) AS is_territory,
            i.latitude,
            i.longitude,
            c.cipcode,
                CASE
                    WHEN (c.cipcode ~~ '01.%'::text) THEN 'Agriculture'::text
                    WHEN (c.cipcode ~~ '03.%'::text) THEN 'Natural Resources & Conservation'::text
                    WHEN (c.cipcode ~~ '04.%'::text) THEN 'Architecture'::text
                    WHEN (c.cipcode ~~ '05.%'::text) THEN 'Area & Ethnic Studies'::text
                    WHEN (c.cipcode ~~ '09.%'::text) THEN 'Communication & Journalism'::text
                    WHEN (c.cipcode ~~ '10.%'::text) THEN 'Communications Technology'::text
                    WHEN (c.cipcode ~~ '11.%'::text) THEN 'Computer & Information Sciences'::text
                    WHEN (c.cipcode ~~ '12.%'::text) THEN 'Personal & Culinary Services'::text
                    WHEN (c.cipcode ~~ '13.%'::text) THEN 'Education'::text
                    WHEN (c.cipcode ~~ '14.%'::text) THEN 'Engineering'::text
                    WHEN (c.cipcode ~~ '15.%'::text) THEN 'Engineering Technology'::text
                    WHEN (c.cipcode ~~ '16.%'::text) THEN 'Foreign Languages & Linguistics'::text
                    WHEN (c.cipcode ~~ '19.%'::text) THEN 'Family & Consumer Sciences'::text
                    WHEN (c.cipcode ~~ '22.%'::text) THEN 'Legal Studies'::text
                    WHEN (c.cipcode ~~ '23.%'::text) THEN 'English Language & Literature'::text
                    WHEN (c.cipcode ~~ '24.%'::text) THEN 'Liberal Arts & General Studies'::text
                    WHEN (c.cipcode ~~ '26.%'::text) THEN 'Biological Sciences'::text
                    WHEN (c.cipcode ~~ '27.%'::text) THEN 'Mathematics & Statistics'::text
                    WHEN (c.cipcode ~~ '29.%'::text) THEN 'Military Technologies'::text
                    WHEN (c.cipcode ~~ '30.%'::text) THEN 'Interdisciplinary Studies'::text
                    WHEN (c.cipcode ~~ '31.%'::text) THEN 'Parks, Recreation & Fitness'::text
                    WHEN (c.cipcode ~~ '38.%'::text) THEN 'Philosophy & Religious Studies'::text
                    WHEN (c.cipcode ~~ '39.%'::text) THEN 'Theology & Religious Vocations'::text
                    WHEN (c.cipcode ~~ '40.%'::text) THEN 'Physical Sciences'::text
                    WHEN (c.cipcode ~~ '41.%'::text) THEN 'Science Technologies'::text
                    WHEN (c.cipcode ~~ '42.%'::text) THEN 'Psychology'::text
                    WHEN (c.cipcode ~~ '43.%'::text) THEN 'Homeland Security & Criminal Justice'::text
                    WHEN (c.cipcode ~~ '44.%'::text) THEN 'Public Administration & Social Services'::text
                    WHEN (c.cipcode ~~ '45.%'::text) THEN 'Social Sciences'::text
                    WHEN (c.cipcode ~~ '46.%'::text) THEN 'Construction Trades'::text
                    WHEN (c.cipcode ~~ '47.%'::text) THEN 'Mechanic & Repair Technology'::text
                    WHEN (c.cipcode ~~ '48.%'::text) THEN 'Precision Production'::text
                    WHEN (c.cipcode ~~ '49.%'::text) THEN 'Transportation & Materials Moving'::text
                    WHEN (c.cipcode ~~ '50.%'::text) THEN 'Visual & Performing Arts'::text
                    WHEN (c.cipcode ~~ '51.%'::text) THEN 'Health Professions'::text
                    WHEN (c.cipcode ~~ '52.%'::text) THEN 'Business & Management'::text
                    WHEN (c.cipcode ~~ '54.%'::text) THEN 'History'::text
                    ELSE 'Other'::text
                END AS cip_family,
            c.awlevel,
                CASE c.awlevel
                    WHEN 1 THEN '01. Certificate < 1 year'::text
                    WHEN 20 THEN '02. Certificate < 1 year (clock hour)'::text
                    WHEN 2 THEN '03. Certificate 1-2 years'::text
                    WHEN 21 THEN '04. Certificate 1-2 years (clock hour)'::text
                    WHEN 3 THEN '05. Associate''s degree'::text
                    WHEN 4 THEN '06. Certificate 2-4 years'::text
                    WHEN 5 THEN '07. Bachelor''s degree'::text
                    WHEN 6 THEN '08. Postbaccalaureate certificate'::text
                    WHEN 7 THEN '09. Master''s degree'::text
                    WHEN 8 THEN '10. Post-master''s certificate'::text
                    WHEN 17 THEN '11. Doctor''s degree - research/scholarship'::text
                    WHEN 18 THEN '12. Doctor''s degree - professional practice'::text
                    WHEN 19 THEN '13. Doctor''s degree - other'::text
                    ELSE '99. Unknown'::text
                END AS award_level_desc,
                CASE c.awlevel
                    WHEN 3 THEN 1
                    WHEN 1 THEN 2
                    WHEN 20 THEN 3
                    WHEN 2 THEN 4
                    WHEN 21 THEN 5
                    WHEN 4 THEN 6
                    WHEN 5 THEN 7
                    WHEN 6 THEN 8
                    WHEN 7 THEN 9
                    WHEN 8 THEN 10
                    WHEN 17 THEN 11
                    WHEN 18 THEN 12
                    WHEN 19 THEN 13
                    ELSE 99
                END AS award_level_sort,
                CASE c.awlevel
                    WHEN 1 THEN '1. Subbaccalaureate Certificate'::text
                    WHEN 20 THEN '1. Subbaccalaureate Certificate'::text
                    WHEN 2 THEN '1. Subbaccalaureate Certificate'::text
                    WHEN 21 THEN '1. Subbaccalaureate Certificate'::text
                    WHEN 4 THEN '1. Subbaccalaureate Certificate'::text
                    WHEN 3 THEN '2. Associate''s Degree'::text
                    WHEN 5 THEN '3. Bachelor''s Degree'::text
                    WHEN 6 THEN '4. Postbaccalaureate Certificate'::text
                    WHEN 7 THEN '5. Master''s Degree'::text
                    WHEN 8 THEN '5. Master''s Degree'::text
                    WHEN 17 THEN '6. Doctorate'::text
                    WHEN 18 THEN '6. Doctorate'::text
                    WHEN 19 THEN '6. Doctorate'::text
                    ELSE '7. Unknown'::text
                END AS award_level_tier,
            c.total_completions,
            c.total_men,
            c.total_women,
            c.aian_total,
            c.asian_total,
            c.black_total,
            c.hispanic_total,
            c.nhpi_total,
            c.white_total,
            c.two_or_more_total,
            c.unknown_total,
            c.nonresident_total,
            (((((c.aian_total + c.asian_total) + c.black_total) + c.hispanic_total) + c.nhpi_total) + c.two_or_more_total) AS students_of_color_total,
            round((((c.black_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_black,
            round((((c.hispanic_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_hispanic,
            round((((c.white_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_white,
            round((((c.asian_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_asian,
            round((((c.aian_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_aian,
            round((((c.nhpi_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_nhpi,
            round((((c.two_or_more_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_two_or_more,
            round((((c.unknown_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_unknown,
            round((((c.nonresident_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_nonresident,
            round(((((((((c.aian_total + c.asian_total) + c.black_total) + c.hispanic_total) + c.nhpi_total) + c.two_or_more_total))::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) AS pct_students_of_color,
            (round((((c.white_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1) - round((((c.black_total)::numeric / (NULLIF(c.total_completions, 0))::numeric) * (100)::numeric), 1)) AS white_black_gap
           FROM (completions c
             JOIN institutions i ON ((c.unitid = i.unitid)))
        )
 SELECT unitid,
    institution_name,
    city,
    state,
    county,
    control_type,
    institution_level,
    sector_desc,
    institution_size_desc,
    is_hbcu,
    is_tribal,
    is_hospital,
    is_medical,
    is_territory,
    latitude,
    longitude,
    cipcode,
    cip_family,
    awlevel,
    award_level_desc,
    award_level_sort,
    award_level_tier,
    total_completions,
    total_men,
    total_women,
    aian_total,
    asian_total,
    black_total,
    hispanic_total,
    nhpi_total,
    white_total,
    two_or_more_total,
    unknown_total,
    nonresident_total,
    students_of_color_total,
    pct_black,
    pct_hispanic,
    pct_white,
    pct_asian,
    pct_aian,
    pct_nhpi,
    pct_two_or_more,
    pct_unknown,
    pct_nonresident,
    pct_students_of_color,
    white_black_gap
   FROM joined;


--
-- PostgreSQL database dump complete
--

\unrestrict j1qLltK2hg5ohZuqB6l7KKkpOHOl37HDr0UjXxwKtvGcd6Ug2aflCL3oSPTeuj5

