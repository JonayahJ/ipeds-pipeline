with source as (
    select * from {{ source('ipeds', 'raw_completions_2024') }}
),

cleaned as (
    select
        unitid,
        cipcode,
        majornum,
        awlevel,
        ctotalt as total_completions,
        ctotalm as total_men,
        ctotalw as total_women,
        caiant  as aian_total,
        casiat  as asian_total,
        cbkaat  as black_total,
        chispt  as hispanic_total,
        cnhpit  as nhpi_total,
        cwhitt  as white_total,
        c2mort  as two_or_more_total,
        cunknt  as unknown_total,
        cnralt  as nonresident_total
    from source
    where ctotalt > 0
)

select * from cleaned
