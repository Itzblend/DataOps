{{
    config(
        materialized='incremental'
    )
}}

with source_github_issues as (
    select
        data ->> 'url' AS url,
        data ->> 'id' AS id,
        data ->> 'node_id' AS node_id,
        (data ->> 'number')::BIGINT AS number,
        data ->> 'title' AS title,
        data -> 'user' ->> 'id' AS user_id,
        data -> 'labels' ->> 'id' AS label_id,
        data ->> 'state' AS state,
        data ->> 'locked' AS locked,
        data ->> 'assignee' AS assignee,
        data ->> 'milestone' AS milestone,
        (data ->> 'comments')::INT AS comments,
        (data ->> 'created_at')::TIMESTAMP AS created_at,
        (data ->> 'updated_at')::TIMESTAMP AS updated_at,
        (data ->> 'closed_at')::TIMESTAMP AS closed_at,
        data ->> 'author_association' AS author_association,
        data ->> 'active_lock_reason' AS active_lock_reason,
        data ->> 'body' AS body,
        data -> 'reactions' AS reactions,
        data -> 'timeline_url' AS timeline_url
    from {{ source('issues_json', 'issues_json') }}
),

    final as (
        select * from source_github_issues
    )

select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}