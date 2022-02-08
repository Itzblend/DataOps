{{
    config(
        materialized='incremental'
    )
}}

with source_github_pulls as (
    select
        data ->> 'url' AS url,
        data ->> 'id' AS id,
        data ->> 'node_id' AS node_id,
        (data ->> 'number')::BIGINT AS number,
        data ->> 'state' AS state,
        data ->> 'locked' AS locked,
        data ->> 'title' AS title,
        data -> 'user' ->> 'id' AS user_id,
        data ->> 'body' AS body,
        (data ->> 'created_at')::TIMESTAMP AS created_at,
        (data ->> 'updated_at')::TIMESTAMP AS updated_at,
        (data ->> 'closed_at')::TIMESTAMP AS closed_at,
        (data ->> 'merged_at')::TIMESTAMP AS merged_at,
        data ->> 'merge_commit_sha' AS merge_commit_sha,
        data ->> 'assignee' AS assignee,
        data ->> 'milestone' AS milestone,
        data ->> 'dragt' AS draft,
        data -> 'head' ->> 'sha' AS head_sha,
        (data -> 'repo' ->> 'id')::BIGINT AS repo_id,
        data ->> 'author_association' AS author_association,
        data ->> 'auto_merge' AS auto_merge,
        data ->> 'active_lock_reason' AS active_lock_reason
    from {{ source('pulls_json', 'pulls_json') }}
),

    final as (
        select * from source_github_pulls
    )

select * from final

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}