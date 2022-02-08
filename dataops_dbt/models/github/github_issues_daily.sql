{{
    config(
        materialized='view'
    )
}}

with source_github_issues as (
    select
        date_trunc('day', created_at) as date,
        count(*)
    from {{ ref('github_issues') }}
    group by date_trunc('day', created_at)
),

    final as (
        select * from source_github_issues
    )

select * from final