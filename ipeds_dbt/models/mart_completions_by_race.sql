with completions as (
    select * from {{ ref('stg_completions_2024') }}
    where cipcode != '99'
),

institutions as (
    select * from {{ ref('stg_institutions_2024') }}
),

joined as (
    select
        -- institution details
        i.unitid,
        i.institution_name,
        i.city,
        i.state,
        i.county,
        i.control_type,
        i.institution_level,
        i.sector_desc,
        i.institution_size_desc,
        i.hbcu = 1                          as is_hbcu,
        i.tribal = 1                        as is_tribal,
        i.hospital = 1                      as is_hospital,
        i.medical = 1                       as is_medical,
        i.state in ('PR','GU','VI','MP','AS') as is_territory,
        i.latitude,
        i.longitude,

        -- program details
        c.cipcode,
        case
            when c.cipcode like '01.%' then 'Agriculture'
            when c.cipcode like '03.%' then 'Natural Resources & Conservation'
            when c.cipcode like '04.%' then 'Architecture'
            when c.cipcode like '05.%' then 'Area & Ethnic Studies'
            when c.cipcode like '09.%' then 'Communication & Journalism'
            when c.cipcode like '10.%' then 'Communications Technology'
            when c.cipcode like '11.%' then 'Computer & Information Sciences'
            when c.cipcode like '12.%' then 'Personal & Culinary Services'
            when c.cipcode like '13.%' then 'Education'
            when c.cipcode like '14.%' then 'Engineering'
            when c.cipcode like '15.%' then 'Engineering Technology'
            when c.cipcode like '16.%' then 'Foreign Languages & Linguistics'
            when c.cipcode like '19.%' then 'Family & Consumer Sciences'
            when c.cipcode like '22.%' then 'Legal Studies'
            when c.cipcode like '23.%' then 'English Language & Literature'
            when c.cipcode like '24.%' then 'Liberal Arts & General Studies'
            when c.cipcode like '26.%' then 'Biological Sciences'
            when c.cipcode like '27.%' then 'Mathematics & Statistics'
            when c.cipcode like '29.%' then 'Military Technologies'
            when c.cipcode like '30.%' then 'Interdisciplinary Studies'
            when c.cipcode like '31.%' then 'Parks, Recreation & Fitness'
            when c.cipcode like '38.%' then 'Philosophy & Religious Studies'
            when c.cipcode like '39.%' then 'Theology & Religious Vocations'
            when c.cipcode like '40.%' then 'Physical Sciences'
            when c.cipcode like '41.%' then 'Science Technologies'
            when c.cipcode like '42.%' then 'Psychology'
            when c.cipcode like '43.%' then 'Homeland Security & Criminal Justice'
            when c.cipcode like '44.%' then 'Public Administration & Social Services'
            when c.cipcode like '45.%' then 'Social Sciences'
            when c.cipcode like '46.%' then 'Construction Trades'
            when c.cipcode like '47.%' then 'Mechanic & Repair Technology'
            when c.cipcode like '48.%' then 'Precision Production'
            when c.cipcode like '49.%' then 'Transportation & Materials Moving'
            when c.cipcode like '50.%' then 'Visual & Performing Arts'
            when c.cipcode like '51.%' then 'Health Professions'
            when c.cipcode like '52.%' then 'Business & Management'
            when c.cipcode like '54.%' then 'History'
            else 'Other'
        end                                 as cip_family,

        -- award level
        c.awlevel,
        case c.awlevel
            when 1  then 'Certificate < 1 year'
            when 2  then 'Certificate 1-2 years'
            when 3  then 'Associate''s degree'
            when 4  then 'Certificate 2-4 years'
            when 5  then 'Bachelor''s degree'
            when 6  then 'Postbaccalaureate certificate'
            when 7  then 'Master''s degree'
            when 8  then 'Post-master''s certificate'
            when 17 then 'Doctor''s degree - research/scholarship'
            when 18 then 'Doctor''s degree - professional practice'
            when 19 then 'Doctor''s degree - other'
            when 20 then 'Certificate < 1 year (clock hour)'
            when 21 then 'Certificate 1-2 years (clock hour)'
            else 'Unknown'
        end                                 as award_level_desc,

        -- award level sort
        case c.awlevel
            when 3  then 1
            when 1  then 2
            when 20 then 3
            when 2  then 4
            when 21 then 5
            when 4  then 6
            when 5  then 7
            when 6  then 8
            when 7  then 9
            when 8  then 10
            when 17 then 11
            when 18 then 12
            when 19 then 13
            else 99
        end                                 as award_level_sort,

        -- raw completion counts
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

        -- students of color total
        c.aian_total +
        c.asian_total +
        c.black_total +
        c.hispanic_total +
        c.nhpi_total +
        c.two_or_more_total                 as students_of_color_total,

        -- percentages
        round(c.black_total::numeric    / nullif(c.total_completions, 0) * 100, 1) as pct_black,
        round(c.hispanic_total::numeric / nullif(c.total_completions, 0) * 100, 1) as pct_hispanic,
        round(c.white_total::numeric    / nullif(c.total_completions, 0) * 100, 1) as pct_white,
        round(c.asian_total::numeric    / nullif(c.total_completions, 0) * 100, 1) as pct_asian,
        round(c.aian_total::numeric     / nullif(c.total_completions, 0) * 100, 1) as pct_aian,
        round(c.nhpi_total::numeric     / nullif(c.total_completions, 0) * 100, 1) as pct_nhpi,
        round(c.two_or_more_total::numeric / nullif(c.total_completions, 0) * 100, 1) as pct_two_or_more,
        round(c.unknown_total::numeric  / nullif(c.total_completions, 0) * 100, 1) as pct_unknown,
        round(c.nonresident_total::numeric / nullif(c.total_completions, 0) * 100, 1) as pct_nonresident,
        round((c.aian_total +
               c.asian_total +
               c.black_total +
               c.hispanic_total +
               c.nhpi_total +
               c.two_or_more_total)::numeric / nullif(c.total_completions, 0) * 100, 1) as pct_students_of_color,

        -- gap calculation
        round(c.white_total::numeric    / nullif(c.total_completions, 0) * 100, 1) -
        round(c.black_total::numeric    / nullif(c.total_completions, 0) * 100, 1) as white_black_gap

    from completions c
    join institutions i on c.unitid = i.unitid
)

select * from joined
