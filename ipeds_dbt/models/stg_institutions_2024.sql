with source as (
    select * from {{ source('ipeds', 'raw_institutions_2024') }}
),

cleaned as (
    select
        unitid,
        instnm                          as institution_name,
        addr                            as address,
        city,
        stabbr                          as state,
        zip,
        webaddr                         as website,
        countynm                        as county,
        latitude,
        longitud                        as longitude,
        hbcu,
        tribal,
        hospital,
        medical,
        deggrant                        as degree_granting,
        c21basic                        as carnegie_classification,
        instsize                        as institution_size,
        case control
            when 1 then 'Public'
            when 2 then 'Private Non-Profit'
            when 3 then 'Private For-Profit'
            else 'Unknown'
        end                             as control_type,
        case iclevel
            when 1 then '4-year'
            when 2 then '2-year'
            when 3 then 'Less than 2-year'
            else 'Unknown'
        end                             as institution_level,
        case sector
            when 0 then 'Administrative Unit'
            when 1 then 'Public 4-year'
            when 2 then 'Private Non-Profit 4-year'
            when 3 then 'Private For-Profit 4-year'
            when 4 then 'Public 2-year'
            when 5 then 'Private Non-Profit 2-year'
            when 6 then 'Private For-Profit 2-year'
            when 7 then 'Public Less than 2-year'
            when 8 then 'Private Non-Profit Less than 2-year'
            when 9 then 'Private For-Profit Less than 2-year'
            else 'Unknown'
        end                             as sector_desc,
        case instsize
            when 1 then 'Under 1,000'
            when 2 then '1,000 - 4,999'
            when 3 then '5,000 - 9,999'
            when 4 then '10,000 - 19,999'
            when 5 then '20,000 and above'
            else 'Unknown'
        end                             as institution_size_desc
    from source
)

select * from cleaned
