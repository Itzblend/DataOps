{{
    config(
        materialized='view'
    )
}}

with source_github_pulls as (
    select
        date_trunc('day', created_at) as date,
        count(*)
    from {{ ref('github_pulls') }}
    group by date_trunc('day', created_at)
),

    final as (
        select * from source_github_pulls
    )

select * from final